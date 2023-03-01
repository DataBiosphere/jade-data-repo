package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.service.auth.iam.exception.IamUnavailableException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.BucketLockFailureException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.google.cloud.Binding;
import com.google.cloud.Policy;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.Autoclass;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class GoogleBucketService {
  private static final Logger logger = LoggerFactory.getLogger(GoogleBucketService.class);

  @VisibleForTesting
  public static final String STORAGE_OBJECT_VIEWER_ROLE = "roles/storage.objectViewer";

  public static final String STORAGE_LEGACY_BUCKET_WRITER_ROLE = "roles/storage.legacyBucketWriter";

  private final GoogleResourceDao resourceDao;
  private final GcsProjectFactory gcsProjectFactory;
  private final ConfigurationService configService;

  @Autowired private Environment env;

  @Autowired
  public GoogleBucketService(
      GoogleResourceDao resourceDao,
      GcsProjectFactory gcsProjectFactory,
      ConfigurationService configService) {
    this.resourceDao = resourceDao;
    this.gcsProjectFactory = gcsProjectFactory;
    this.configService = configService;
  }

  /**
   * Fetch metadata for an existing bucket_resource Note this method checks for the existence of the
   * underlying cloud resource, if checkCloudResourceExists=true.
   *
   * @param bucketResourceId id of the bucket resource
   * @param checkCloudResourceExists true to do the existence check, false to skip it
   * @return a reference to the bucket as a POJO GoogleBucketResource
   * @throws GoogleResourceNotFoundException if no bucket_resource metadata row is found
   * @throws CorruptMetadataException if the bucket_resource metadata row exists but the cloud
   *     resource does not
   */
  public GoogleBucketResource getBucketResourceById(
      UUID bucketResourceId, boolean checkCloudResourceExists) {
    // fetch the bucket_resource metadata row
    GoogleBucketResource bucketResource = resourceDao.retrieveBucketById(bucketResourceId);

    if (checkCloudResourceExists) {
      // throw an exception if the bucket does not already exist
      Bucket bucket = getCloudBucket(bucketResource.getName());
      if (bucket == null) {
        throw new CorruptMetadataException(
            "Bucket metadata exists, but bucket not found: " + bucketResource.getName());
      }
    }

    return bucketResource;
  }

  /**
   * Fetch/create a bucket cloud resource and the associated metadata in the bucket_resource table.
   *
   * <p>On entry to this method, there are 9 states along 3 main dimensions: Google Bucket - exists
   * or not DR Metadata record - exists or not DR Metadata lock state (only if record exists): - not
   * locked - locked by this flight - locked by another flight In addition, there is one case where
   * it matters if we are reusing buckets or not.
   *
   * <p>Itemizing the 9 cases: CASE 1: bucket exists, record exists, record is unlocked The
   * predominant case. We return the bucket resource
   *
   * <p>CASE 2: bucket exists, record exists, locked by another flight We have to wait until the
   * other flight finishes creating the bucket. Throw BucketLockFailureException. We expect the
   * calling Step to retry on that exception.
   *
   * <p>CASE 3: bucket exists, record exists, locked by us This flight created the bucket, but
   * failed before we could unlock it. So, we unlock and return the bucket resource.
   *
   * <p>CASE 4: bucket exists, no record exists, we are allowed to reuse buckets This is a common
   * case in development where we re-use the same cloud resources over and over during testing
   * rather than continually create and destroy them. In this case, we proceed with the
   * try-to-create-bucket-metadata algorithm.
   *
   * <p>CASE 5: bucket exists, no record exists, we are not reusing buckets This is the production
   * mode and should not happen. It means we our metadata does not reflect the actual cloud
   * resources. Throw CorruptMetadataException
   *
   * <p>CASE 6: no bucket exists, record exists, not locked This should not happen. Throw
   * CorruptMetadataException
   *
   * <p>CASE 7: no bucket exists, record exists, locked by another flight We have to wait until the
   * other flight finishes creating the bucket. Throw BucketLockFailureException. We expect the
   * calling Step to retry on that exception.
   *
   * <p>CASE 8: no bucket exists, record exists, locked by this flight We must have failed after
   * creating and locking the record, but before creating the bucket. Proceed with the
   * finish-trying-to-create-bucket algorithm
   *
   * <p>CASE 9: no bucket exists, no record exists Proceed with try-to-create-bucket algorithm
   *
   * <p>The algorithm to create a bucket is like a miniature flight and we implement it as a set of
   * methods that chain to make the whole algorithm: 1. createMetadataRecord: create and lock the
   * metadata record; then 2. createCloudBucket: if the bucket does not exist, create it; then 3.
   * createFinish: unlock the metadata record The algorithm may fail between any of those steps, so
   * we may arrive in this method needing to do some or all of those steps.
   *
   * @param bucketName name for a new or existing bucket
   * @param projectResource project in which the bucket should be retrieved or created
   * @param flightId flight making the request
   * @param daysToLive an optional retention period for the bucket
   * @return a reference to the bucket as a POJO GoogleBucketResource
   * @throws CorruptMetadataException in CASE 5 and CASE 6
   * @throws BucketLockFailureException in CASE 2 and CASE 7, and sometimes case 9
   */
  public GoogleBucketResource getOrCreateBucket(
      String bucketName,
      GoogleProjectResource projectResource,
      GoogleRegion region,
      String flightId,
      Duration daysToLive,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount,
      boolean autoclassEnabled)
      throws InterruptedException {

    boolean allowReuseExistingBuckets =
        configService.getParameterValue(ConfigEnum.ALLOW_REUSE_EXISTING_BUCKETS);
    logger.info("application property allowReuseExistingBuckets = " + allowReuseExistingBuckets);

    // Try to get the bucket record and the bucket object
    GoogleBucketResource googleBucketResource =
        resourceDao.getBucket(bucketName, projectResource.getId());
    Bucket bucket = getCloudBucket(bucketName);
    // Test all of the cases
    if (bucket != null) {
      if (googleBucketResource != null) {
        String lockingFlightId = googleBucketResource.getFlightId();
        if (lockingFlightId == null) {
          // CASE 1: everything exists and is unlocked
          return googleBucketResource;
        }
        if (!StringUtils.equals(lockingFlightId, flightId)) {
          // CASE 2: another flight is creating the bucket
          throw bucketLockException(lockingFlightId);
        }
        // CASE 3: we have the flight locked, but we did all of the creating.
        return createFinish(bucket, flightId, googleBucketResource);
      } else {
        // bucket exists, but metadata record does not exist.
        if (allowReuseExistingBuckets) {
          // CASE 4: go ahead and reuse the bucket and its location
          return createMetadataRecord(
              bucketName,
              projectResource,
              region,
              flightId,
              daysToLive,
              getReaderGroups,
              dedicatedServiceAccount,
              autoclassEnabled);
        } else {
          // CASE 5:
          throw new CorruptMetadataException(
              "Bucket already exists, metadata out of sync with cloud state: " + bucketName);
        }
      }
    } else {
      // bucket does not exist
      if (googleBucketResource != null) {
        String lockingFlightId = googleBucketResource.getFlightId();
        if (lockingFlightId == null) {
          // CASE 6: no bucket, but the metadata record exists unlocked
          throw new CorruptMetadataException(
              "Bucket does not exist, metadata out of sync with cloud state: " + bucketName);
        }
        if (!StringUtils.equals(lockingFlightId, flightId)) {
          // CASE 7: another flight is creating the bucket
          throw bucketLockException(lockingFlightId);
        }
        // CASE 8: this flight has the metadata locked, but didn't finish creating the bucket
        return createCloudBucket(
            googleBucketResource,
            flightId,
            daysToLive,
            getReaderGroups,
            dedicatedServiceAccount,
            autoclassEnabled);
      } else {
        // CASE 9: no bucket and no record
        return createMetadataRecord(
            bucketName,
            projectResource,
            region,
            flightId,
            daysToLive,
            getReaderGroups,
            dedicatedServiceAccount,
            autoclassEnabled);
      }
    }
  }

  private BucketLockException bucketLockException(String flightId) {
    return new BucketLockException("Bucket locked by flightId: " + flightId);
  }

  // Step 1 of creating a new bucket - create and lock the metadata record
  private GoogleBucketResource createMetadataRecord(
      String bucketName,
      GoogleProjectResource projectResource,
      GoogleRegion region,
      String flightId,
      Duration daysToLive,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount,
      boolean autoclassEnabled)
      throws InterruptedException {

    // insert a new bucket_resource row and lock it
    GoogleBucketResource googleBucketResource =
        resourceDao.createAndLockBucket(
            bucketName, projectResource, region, flightId, autoclassEnabled);
    if (googleBucketResource == null) {
      // We tried and failed to get the lock. So we ended up in CASE 2 after all.
      GoogleBucketResource lockingGoogleBucketResource =
          resourceDao.getBucket(bucketName, projectResource.getId());
      if (lockingGoogleBucketResource != null) {
        throw bucketLockException(lockingGoogleBucketResource.getFlightId());
      } else {
        throw new CorruptMetadataException(
            String.format(
                "Could not create and lock bucket, but no locking row exists. FlightId: %s",
                flightId));
      }
    }

    // this fault is used by the ResourceLockTest
    if (configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("BUCKET_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("BUCKET_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    return createCloudBucket(
        googleBucketResource,
        flightId,
        daysToLive,
        getReaderGroups,
        dedicatedServiceAccount,
        autoclassEnabled);
  }

  // Step 2 of creating a new bucket
  private GoogleBucketResource createCloudBucket(
      GoogleBucketResource bucketResource,
      String flightId,
      Duration daysToLive,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount,
      boolean enableAutoclass) {
    // If the bucket doesn't exist, create it
    Bucket bucket = getCloudBucket(bucketResource.getName());
    if (bucket == null) {
      bucket =
          newCloudBucket(
              bucketResource,
              daysToLive,
              getReaderGroups,
              dedicatedServiceAccount,
              enableAutoclass);
    }
    return createFinish(bucket, flightId, bucketResource);
  }

  // Step 3 (last) of creating a new bucket
  private GoogleBucketResource createFinish(
      Bucket bucket, String flightId, GoogleBucketResource bucketResource) {
    resourceDao.unlockBucket(bucket.getName(), flightId);
    Acl.Entity owner = bucket.getOwner();
    logger.info("bucket is owned by '{}'", owner.toString());

    return bucketResource;
  }

  /**
   * Update the bucket_resource metadata table to match the state of the underlying cloud. - If the
   * bucket exists, then the metadata row should also exist and be unlocked. - If the bucket does
   * not exist, then the metadata row should not exist. If the metadata row is locked, then only the
   * locking flight can unlock or delete the row.
   *
   * @param bucketName name of the bucket to update
   * @param flightId flight doing the updating
   */
  public void updateBucketMetadata(String bucketName, String flightId) {
    // check if the bucket already exists
    Bucket existingBucket = getCloudBucket(bucketName);
    if (existingBucket != null) {
      // bucket EXISTS. unlock the metadata row
      resourceDao.unlockBucket(bucketName, flightId);
    } else {
      // bucket DOES NOT EXIST. delete the metadata row
      resourceDao.deleteBucketMetadata(bucketName, flightId);
    }
  }

  /**
   * Create a new bucket cloud resource. Note this method does not create any associated metadata in
   * the bucket_resource table.
   *
   * @param bucketResource description of the bucket resource to be created
   * @param daysToLive An optional amount of days to live for items in the bucket
   * @param getReaderGroups A Callable to get reader groups that should be added to the bucket. This
   *     is a Callable so that we don't make extraneous calls to SAM every time we get a bucket and
   *     only call SAM when we actually create the bucket.
   * @param dedicatedServiceAccount If not null, the dataset-specific service account associated
   *     with the dataset that this bucket will be associated with
   * @return a reference to the bucket as a GCS Bucket object
   */
  private Bucket newCloudBucket(
      GoogleBucketResource bucketResource,
      Duration daysToLive,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount,
      Boolean enableAutoclass) {
    final List<String> readerGroups;
    try {
      if (getReaderGroups != null) {
        readerGroups = getReaderGroups.call();
      } else {
        readerGroups = null;
      }
    } catch (Exception e) {
      throw new IamUnavailableException("Could not get policy groups from SAM", e);
    }
    boolean doVersioning =
        Arrays.stream(env.getActiveProfiles()).noneMatch(env -> env.contains("test"));
    String bucketName = bucketResource.getName();
    GoogleRegion region = bucketResource.getRegion();
    List<BucketInfo.LifecycleRule> lifecycleRules = null;
    if (daysToLive != null) {
      int days = (int) daysToLive.toDays();
      lifecycleRules =
          List.of(
              new BucketInfo.LifecycleRule(
                  BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
                  BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(days).build()));
    }
    BucketInfo bucketInfo =
        BucketInfo.newBuilder(bucketName)
            // .setRequesterPays()
            .setAutoclass(Autoclass.newBuilder().setEnabled(enableAutoclass).build())
            .setLocation(region.toString())
            .setVersioningEnabled(doVersioning)
            .setLifecycleRules(lifecycleRules)
            .build();

    GoogleProjectResource projectResource = bucketResource.getProjectResource();
    String googleProjectId = projectResource.getGoogleProjectId();
    GcsProject gcsProject = gcsProjectFactory.get(googleProjectId, true);

    // the project will have been created before this point, so no need to fetch it
    logger.info("Creating bucket '{}' in project '{}'", bucketName, googleProjectId);
    Storage storage = gcsProject.getStorage();
    Bucket createdBucket = storage.create(bucketInfo);

    grantBucketReaderIam(createdBucket, storage, readerGroups, dedicatedServiceAccount);

    return createdBucket;
  }

  public void grantBucketReaderIam(
      Bucket bucket, Storage storage, List<String> readerGroups, String dedicatedServiceAccount) {
    String bucketName = bucket.getName();

    // Not sure why it needs to be version 3, but that's what the Google documentation says.
    // https://cloud.google.com/storage/docs/access-control/using-iam-permissions#code-samples
    Policy originalPolicy =
        storage.getIamPolicy(bucketName, Storage.BucketSourceOption.requestedPolicyVersion(3));
    List<Binding> bindings = new ArrayList<>(originalPolicy.getBindingsList());

    if (readerGroups != null) {
      for (String policy : readerGroups) {
        Binding.Builder newMemberBindingBuilder = Binding.newBuilder();
        newMemberBindingBuilder
            .setRole(STORAGE_OBJECT_VIEWER_ROLE)
            .setMembers(List.of(String.format("group:%s", policy)));
        bindings.add(newMemberBindingBuilder.build());
      }
    }
    if (dedicatedServiceAccount != null) {
      Binding.Builder newMemberBindingBuilder = Binding.newBuilder();
      newMemberBindingBuilder
          .setRole(STORAGE_LEGACY_BUCKET_WRITER_ROLE)
          .setMembers(List.of(String.format("serviceAccount:%s", dedicatedServiceAccount)));
      bindings.add(newMemberBindingBuilder.build());
    }
    Policy.Builder updatedPolicyBuilder = originalPolicy.toBuilder();
    updatedPolicyBuilder.setBindings(bindings).setVersion(3);
    storage.setIamPolicy(bucketName, updatedPolicyBuilder.build());
  }

  /**
   * Fetch an existing bucket cloud resource. Note this method does not check any associated
   * metadata in the bucket_resource table.
   *
   * @param bucketName name of the bucket to retrieve
   * @return a reference to the bucket as a GCS Bucket object, null if not found
   */
  @VisibleForTesting
  public Bucket getCloudBucket(String bucketName) {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    try {
      return storage.get(bucketName);
    } catch (StorageException e) {
      throw new GoogleResourceException("Could not check bucket existence", e);
    }
  }
}
