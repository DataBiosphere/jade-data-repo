package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.CollectionType;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.load.LoadDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.BucketResourceLockTester;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.google.api.client.util.Lists;
import com.google.cloud.Binding;
import com.google.cloud.Policy;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class BucketResourceTest {
  private final Logger logger = LoggerFactory.getLogger(BucketResourceTest.class);

  @Autowired private LoadDao loadDao;
  @Autowired private ConfigurationService configService;
  @Autowired private DatasetBucketDao datasetBucketDao;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private GoogleBucketService bucketService;
  @Autowired private GoogleProjectService projectService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private GoogleResourceDao resourceDao;
  @Autowired private ProfileService profileService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BufferService bufferService;

  @MockBean private IamProviderInterface samService;

  private BucketResourceUtils bucketResourceUtils = new BucketResourceUtils();
  private BillingProfileModel profile;
  private Storage storage;
  private List<GoogleBucketResource> bucketResources;
  private boolean allowReuseExistingBuckets;
  private GoogleProjectResource projectResource;
  private UUID datasetId;

  @Before
  public void setup() throws Exception {
    logger.info(
        "property allowReuseExistingBuckets = {}",
        bucketResourceUtils.getAllowReuseExistingBuckets(configService));

    configService.reset();
    profile = connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    connectedOperations.stubOutSamCalls(samService);
    storage = StorageOptions.getDefaultInstance().getService();
    bucketResources = new ArrayList<>();

    // get or created project in which to do the bucket work
    projectResource = buildProjectResource();

    String resourcePath = "snapshot-test-dataset.json";
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(profile.getId());

    DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);
    datasetId = summaryModel.getId();
  }

  @After
  public void teardown() throws Exception {
    for (GoogleBucketResource bucketResource : bucketResources) {
      deleteBucket(bucketResource);
    }
    // Connected operations resets the configuration
    connectedOperations.teardown();
  }

  @Test
  // create and delete the bucket, checking that the metadata and cloud state match what is expected
  public void createAndDeleteBucketTest() throws Exception {
    String bucketName = "testbucket_createanddeletebuckettest";
    String flightId = "createAndDeleteBucketTest";

    // create the bucket and metadata
    GoogleBucketResource bucketResource =
        createBucket(
            bucketName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightId,
            null,
            null,
            null);

    // check the bucket and metadata exist
    checkBucketExists(bucketResource.getResourceId());

    // delete the bucket and metadata
    deleteBucket(bucketResource);
    checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
  }

  @Test
  // create buckets in different regions and see if they're actually created there.
  public void createBucketsInDifferentRegionsTest() throws Exception {
    for (GoogleRegion region : List.of(GoogleRegion.US_CENTRAL1, GoogleRegion.US_EAST1)) {
      String bucketName = "testbucket_bucketregionstest_" + region;
      String flightId = "bucketRegionsTest_" + region;

      // create the bucket and metadata
      GoogleBucketResource bucketResource =
          createBucket(bucketName, projectResource, region, flightId, null, null, null);

      // Get the Bucket
      Bucket cloudBucket = bucketService.getCloudBucket(bucketName);

      assertThat(
          "Google bucket was created in " + region,
          cloudBucket.getLocation(),
          equalToIgnoringCase(region.toString()));
    }
  }

  @Test
  // two threads compete for the bucket lock, confirm one wins and one loses. a third thread fetches
  // the bucket
  // after it's been created, confirm it succeeds.
  public void twoThreadsCompeteForLockTest() throws Exception {
    String flightIdBase = "twoThreadsCompeteForLockTest";
    String bucketName = "twothreadscompeteforlocktest";
    GoogleRegion bucketRegion = GoogleRegion.DEFAULT_GOOGLE_REGION;

    BucketResourceLockTester resourceLockA =
        new BucketResourceLockTester(
            bucketService,
            datasetBucketDao,
            datasetId,
            bucketName,
            projectResource,
            bucketRegion,
            flightIdBase + "A",
            true);
    BucketResourceLockTester resourceLockB =
        new BucketResourceLockTester(
            bucketService,
            datasetBucketDao,
            datasetId,
            bucketName,
            projectResource,
            bucketRegion,
            flightIdBase + "B",
            false);
    BucketResourceLockTester resourceLockC =
        new BucketResourceLockTester(
            bucketService,
            datasetBucketDao,
            datasetId,
            bucketName,
            projectResource,
            bucketRegion,
            flightIdBase + "C",
            false);

    Thread threadA = new Thread(resourceLockA);
    Thread threadB = new Thread(resourceLockB);
    Thread threadC = new Thread(resourceLockC);

    configService.setFault(ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT.name(), true);
    threadA.start();
    TimeUnit.SECONDS.sleep(1);

    threadB.start();
    threadB.join();
    assertTrue("Thread B did get a lock exception", resourceLockB.gotLockException());

    configService.setFault(ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    threadA.join();
    assertFalse("Thread A did not get a lock exception", resourceLockA.gotLockException());

    GoogleBucketResource bucketResource = resourceLockA.getBucketResource();
    bucketResources.add(bucketResource);
    assertNotNull("Thread A did create the bucket", bucketResource);

    checkBucketExists(bucketResource.getResourceId());

    threadC.start();
    threadC.join();
    assertFalse("Thread C did not get a lock exception", resourceLockC.gotLockException());
    assertNotNull("Thread C did get the bucket", resourceLockC.getBucketResource());

    deleteBucket(bucketResource);
    checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
  }

  @Test
  // this is testing one case of corrupt metadata (i.e. state of the cloud does not match state of
  // the metadata)
  // bucket cloud resource exists, but the corresponding bucket_resource metadata row does not
  public void bucketExistsBeforeMetadataTest() throws Exception {
    logger.info(
        "property allowReuseExistingBuckets = {}",
        bucketResourceUtils.getAllowReuseExistingBuckets(configService));

    String bucketName = "testbucket_bucketexistsbeforemetadatatest";
    String flightIdA = "bucketExistsBeforeMetadataTestA";

    // create the bucket and metadata
    GoogleBucketResource bucketResource =
        createBucket(
            bucketName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightIdA,
            null,
            null,
            null);
    checkBucketExists(bucketResource.getResourceId());

    // delete the metadata only
    boolean rowDeleted = resourceDao.deleteBucketMetadata(bucketName, flightIdA);
    assertTrue("metadata row deleted", rowDeleted);

    // try to fetch the bucket again, check fails with not found exception
    boolean caughtNotFoundException = false;
    try {
      bucketService.getBucketResourceById(bucketResource.getResourceId(), true);
    } catch (GoogleResourceNotFoundException cmEx) {
      caughtNotFoundException = true;
    }
    assertTrue("fetch failed when metadata does not exist", caughtNotFoundException);

    // set application property allowReuseExistingBuckets=false
    // try to create bucket again, check fails with corrupt metadata exception
    bucketResourceUtils.setAllowReuseExistingBuckets(configService, false);
    String flightIdB = "bucketExistsBeforeMetadataTestB";
    boolean caughtCorruptMetadataException = false;
    try {
      createBucket(
          bucketName,
          projectResource,
          GoogleRegion.DEFAULT_GOOGLE_REGION,
          flightIdB,
          null,
          null,
          null);
    } catch (CorruptMetadataException cmEx) {
      caughtCorruptMetadataException = true;
    }
    assertTrue("create failed when cloud resource already exists", caughtCorruptMetadataException);

    // set application property allowReuseExistingBuckets=true
    // try to create bucket again, check succeeds
    bucketResourceUtils.setAllowReuseExistingBuckets(configService, true);
    String flightIdC = "bucketExistsBeforeMetadataTestC";
    bucketResource =
        createBucket(
            bucketName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightIdC,
            null,
            null,
            null);

    // check the bucket and metadata exist
    checkBucketExists(bucketResource.getResourceId());

    // delete the bucket and metadata
    deleteBucket(bucketResource);
    checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
  }

  @Test
  // this is testing one case of corrupt metadata (i.e. state of the cloud does not match state of
  // the metadata)
  // bucket_resource metadata row exists, but the corresponding bucket cloud resource does not
  public void noBucketButMetadataExistsTest() throws Exception {
    logger.info(
        "property allowReuseExistingBuckets = {}",
        bucketResourceUtils.getAllowReuseExistingBuckets(configService));

    String bucketName = "testbucket_nobucketbutmetadataexiststest";
    String flightIdA = "noBucketButMetadataExistsTestA";

    // create the bucket and metadata
    GoogleBucketResource bucketResource =
        createBucket(
            bucketName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightIdA,
            null,
            null,
            null);
    checkBucketExists(bucketResource.getResourceId());

    // delete the bucket cloud resource only
    Bucket bucket = storage.get(bucketName);
    boolean bucketDeleted = bucket.delete();
    assertTrue("bucket cloud resource deleted", bucketDeleted);

    // try to fetch the bucket again, check fails with corrupt metadata exception
    boolean caughtCorruptMetadataException = false;
    try {
      bucketService.getBucketResourceById(bucketResource.getResourceId(), true);
    } catch (CorruptMetadataException cmEx) {
      caughtCorruptMetadataException = true;
    }
    assertTrue("fetch failed when cloud resource does not exist", caughtCorruptMetadataException);

    // try to getOrCreate bucket again, check fails with corrupt metadata exception
    String flightIdB = "bucketExistsBeforeMetadataTestB";
    caughtCorruptMetadataException = false;
    try {
      createBucket(
          bucketName,
          projectResource,
          GoogleRegion.DEFAULT_GOOGLE_REGION,
          flightIdB,
          null,
          null,
          null);
    } catch (CorruptMetadataException cmEx) {
      caughtCorruptMetadataException = true;
    }
    assertTrue(
        "getOrCreate failed when cloud resource does not exist", caughtCorruptMetadataException);

    // update the metadata to match the cloud state, check that everything is deleted
    bucketService.updateBucketMetadata(bucketName, null);
    checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
  }

  @Test
  public void createBucketWithTtlTest() throws Exception {
    String bucketWithTtlName = "testbucket_bucketwithttl";
    String flightWithTtlId = "bucketwithttltest";

    String bucketWithoutTtlName = "testbucket_bucketwithoutttl";
    String flightWithoutTtlId = "bucketwithoutttltest";

    Integer deleteAge = 1;

    // create the bucket and metadata
    GoogleBucketResource bucketWithTtlResource =
        createBucket(
            bucketWithTtlName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightWithTtlId,
            Duration.ofDays(1),
            null,
            null);

    GoogleBucketResource bucketWithoutTtlResource =
        createBucket(
            bucketWithoutTtlName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightWithoutTtlId,
            null,
            null,
            null);

    // check the bucket and metadata exist
    Bucket bucketWithTtl = storage.get(bucketWithTtlName);
    Bucket bucketWithoutTtl = storage.get(bucketWithoutTtlName);

    var lifecycleRule = bucketWithTtl.getLifecycleRules().get(0);
    var lifecycleAction = lifecycleRule.getAction();
    var lifecycleDeleteAge = lifecycleRule.getCondition().getAge();

    assertThat(
        "Delete lifecycle action was set on the bucket",
        lifecycleAction.getActionType(),
        equalTo(BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction().getActionType()));

    assertThat("Delete lifecycle action is set for 1 day", lifecycleDeleteAge, equalTo(deleteAge));

    assertTrue(
        "Bucket without ttl has no lifecycle rules",
        bucketWithoutTtl.getLifecycleRules().isEmpty());

    // delete the bucket and metadata
    for (var bucketResource : List.of(bucketWithTtlResource, bucketWithoutTtlResource)) {
      deleteBucket(bucketResource);
      checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }
  }

  @Test
  public void testCreateBucketWithReaders() throws Exception {
    String bucketName = "bucket_with_reader_groups";
    String flightId = "bucketWithReaderGroupsTest";
    List<String> readerGroups =
        new ArrayList<>(
            samService.retrievePolicyEmails(null, IamResourceType.DATASET, datasetId).values());

    GoogleBucketResource bucket =
        createBucket(
            bucketName,
            projectResource,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            flightId,
            null,
            () -> readerGroups,
            null);

    Policy iamPolicy =
        storage.getIamPolicy(
            bucket.getName(), Storage.BucketSourceOption.requestedPolicyVersion(3));

    assertThat(
        "Bucket has dataset users as bucket readers",
        iamPolicy.getBindingsList(),
        hasItem(
            Binding.newBuilder()
                .setRole(GoogleBucketService.STORAGE_OBJECT_VIEWER_ROLE)
                .setMembers(List.of("group:jadeteam@broadinstitute.org"))
                .build()));
  }

  private GoogleBucketResource createBucket(
      String bucketName,
      GoogleProjectResource projectResource,
      GoogleRegion bucketRegion,
      String flightId,
      Duration ttl,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount)
      throws InterruptedException {

    GoogleBucketResource bucketResource =
        bucketService.getOrCreateBucket(
            bucketName,
            projectResource,
            bucketRegion,
            flightId,
            ttl,
            getReaderGroups,
            dedicatedServiceAccount);

    bucketResources.add(bucketResource);
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResource.getResourceId());

    return bucketResource;
  }

  private void checkBucketExists(UUID bucketResourceId) {
    // confirm the metadata row is unlocked and the bucket exists
    GoogleBucketResource bucketResource =
        bucketService.getBucketResourceById(bucketResourceId, false);
    assertNotNull("bucket metadata row exists", bucketResource);
    assertNull("bucket metadata is unlocked", bucketResource.getFlightId());

    Bucket bucket = storage.get(bucketResource.getName());
    assertNotNull("bucket exists in the cloud", bucket);
  }

  private void deleteBucket(GoogleBucketResource bucketResource) {
    // delete the bucket and update the metadata
    storage.delete(bucketResource.getName());
    bucketService.updateBucketMetadata(bucketResource.getName(), null);
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResource.getResourceId());
  }

  private void checkBucketDeleted(String bucketName, UUID bucketResourceId) {
    // confirm the bucket and metadata row no longer exist
    Bucket bucket = storage.get(bucketName);
    assertNull("bucket no longer exists", bucket);

    boolean exceptionThrown = false;
    try {
      GoogleBucketResource bucketResource =
          bucketService.getBucketResourceById(bucketResourceId, false);
      logger.info("bucketResource = " + bucketResource);
    } catch (GoogleResourceNotFoundException grnfEx) {
      exceptionThrown = true;
    }
    assertTrue("bucket metadata row no longer exists", exceptionThrown);
  }

  private GoogleProjectResource buildProjectResource() throws Exception {
    String role = "roles/bigquery.jobUser";
    String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
    List<String> stewardsGroupEmailList = Lists.newArrayList();
    stewardsGroupEmailList.add(stewardsGroupEmail);
    Map<String, List<String>> roleToStewardMap = new HashMap<>();
    roleToStewardMap.put(role, stewardsGroupEmailList);

    ResourceInfo resourceInfo = bufferService.handoutResource(false);

    // create project metadata
    return projectService.initializeGoogleProject(
        resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId(),
        profile,
        roleToStewardMap,
        GoogleRegion.DEFAULT_GOOGLE_REGION,
        Map.of("test-name", "bucket-resource-test"),
        CollectionType.DATASET);
  }
}
