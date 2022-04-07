package bio.terra.service.filedata;

import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessMethod.TypeEnum;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
@Component
public class DrsService {

  private final Logger logger = LoggerFactory.getLogger(DrsService.class);

  private static final String DRS_OBJECT_VERSION = "0";
  private static final Duration URL_TTL = Duration.ofMinutes(15);
  // atomic counter that we incr on request arrival and decr on request response
  private final AtomicInteger currentDRSRequests = new AtomicInteger(0);

  private final SnapshotService snapshotService;
  private final FileService fileService;
  private final DrsIdService drsIdService;
  private final IamService samService;
  private final ResourceService resourceService;
  private final ConfigurationService configurationService;
  private final JobService jobService;
  private final PerformanceLogger performanceLogger;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final GcsProjectFactory gcsProjectFactory;

  private final Map<UUID, SnapshotProject> snapshotProjects =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<UUID, SnapshotCacheResult> snapshotCache =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<String, String> samAuthorizations =
      Collections.synchronizedMap(new PassiveExpiringMap<>(1, TimeUnit.MINUTES));

  @Autowired
  public DrsService(
      SnapshotService snapshotService,
      FileService fileService,
      DrsIdService drsIdService,
      IamService samService,
      ResourceService resourceService,
      ConfigurationService configurationService,
      JobService jobService,
      PerformanceLogger performanceLogger,
      AzureBlobStorePdao azureBlobStorePdao,
      GcsProjectFactory gcsProjectFactory) {
    this.snapshotService = snapshotService;
    this.fileService = fileService;
    this.drsIdService = drsIdService;
    this.samService = samService;
    this.resourceService = resourceService;
    this.configurationService = configurationService;
    this.jobService = jobService;
    this.performanceLogger = performanceLogger;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.gcsProjectFactory = gcsProjectFactory;
  }

  private class DrsRequestResource implements AutoCloseable {

    DrsRequestResource() {
      // make sure not too many requests are being made at once
      int podCount = jobService.getActivePodCount();
      int maxDRSLookups = configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX);
      int max = maxDRSLookups / podCount;
      logger.info("Max number of DRS lookups allowed : " + max);
      logger.info("Current number of requests being made : " + currentDRSRequests);

      if (currentDRSRequests.get() >= max) {
        throw new TooManyRequestsException(
            "Too many requests are being made at once. Please try again later.");
      }
      currentDRSRequests.incrementAndGet();
    }

    @Override
    public void close() {
      currentDRSRequests.decrementAndGet();
    }
  }

  /**
   * Look up the DRS object for a DRS object ID.
   *
   * @param authUser the user to authenticate this request for
   * @param drsObjectId the object ID to look up
   * @param expand if false and drsObjectId refers to a bundle, then the returned array contains
   *     only those objects directly contained in the bundle
   * @return the DRS object for this ID
   * @throws IllegalArgumentException if there iis an issue with the object id
   * @throws SnapshotNotFoundException if the snapshot for the DRS object cannot be found
   * @throws TooManyRequestsException if there are too many concurrent DRS lookup requests
   */
  public DRSObject lookupObjectByDrsId(
      AuthenticatedUserRequest authUser, String drsObjectId, Boolean expand) {
    try (DrsRequestResource r = new DrsRequestResource()) {
      DrsId drsId = drsIdService.fromObjectId(drsObjectId);
      SnapshotProject snapshotProject;
      try {
        UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
        // We only look up DRS ids for unlocked snapshots.
        String retrieveTimer = performanceLogger.timerStart();

        snapshotProject = getSnapshotProject(snapshotId);

        performanceLogger.timerEndAndLog(
            retrieveTimer,
            drsObjectId, // not a flight, so no job id
            this.getClass().getName(),
            "snapshotService.retrieveAvailable");
      } catch (IllegalArgumentException ex) {
        throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
      } catch (SnapshotNotFoundException ex) {
        throw new DrsObjectNotFoundException(
            "No snapshot found for DRS object id '" + drsObjectId + "'", ex);
      }

      // Make sure requester is a READER on the snapshot
      String samTimer = performanceLogger.timerStart();

      verifyAuthorization(drsId.getSnapshotId(), authUser);

      performanceLogger.timerEndAndLog(
          samTimer,
          drsObjectId, // not a flight, so no job id
          this.getClass().getName(),
          "samService.verifyAuthorization");

      int depth = (expand ? -1 : 1);

      FSItem fsObject;
      try {
        String lookupTimer = performanceLogger.timerStart();
        fsObject = fileService.lookupSnapshotFSItem(snapshotProject, drsId.getFsObjectId(), depth);

        performanceLogger.timerEndAndLog(
            lookupTimer,
            drsObjectId, // not a flight, so no job id
            this.getClass().getName(),
            "fileService.lookupSnapshotFSItem");
      } catch (InterruptedException ex) {
        throw new FileSystemExecutionException(
            "Unexpected interruption during file system processing", ex);
      }

      if (fsObject instanceof FSFile) {
        return drsObjectFromFSFile((FSFile) fsObject, drsId.getSnapshotId(), authUser);
      } else if (fsObject instanceof FSDir) {
        return drsObjectFromFSDir((FSDir) fsObject, drsId.getSnapshotId());
      }

      throw new IllegalArgumentException("Invalid object type");
    }
  }

  public DRSAccessURL getAccessUrlForObjectId(
      AuthenticatedUserRequest authUser, String objectId, String accessId) {
    DRSObject drsObject = lookupObjectByDrsId(authUser, objectId, false);

    DrsId drsId = drsIdService.fromObjectId(objectId);
    UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
    SnapshotCacheResult cachedSnapshot = getSnapshot(snapshotId);

    BillingProfileModel billingProfileModel = cachedSnapshot.billingProfileModel;

    Supplier<IllegalArgumentException> illegalArgumentExceptionSupplier =
        () -> new IllegalArgumentException("No matching access ID was found for object");

    CloudPlatformWrapper platform = CloudPlatformWrapper.of(billingProfileModel.getCloudPlatform());
    if (platform.isGcp()) {
      DRSAccessMethod matchingAccessMethod =
          getAccessMethodMatchingAccessId(accessId, drsObject, TypeEnum.GS)
              .orElseThrow(illegalArgumentExceptionSupplier);
      return signGoogleUrl(cachedSnapshot.googleProjectId, matchingAccessMethod);
    } else if (platform.isAzure()) {
      getAccessMethodMatchingAccessId(accessId, drsObject, TypeEnum.HTTPS)
          .orElseThrow(illegalArgumentExceptionSupplier);
      try {
        FSItem fsItem =
            fileService.lookupSnapshotFSItem(
                getSnapshotProject(snapshotId), drsId.getFsObjectId(), 1);
        return signAzureUrl(billingProfileModel, fsItem, authUser);
      } catch (InterruptedException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      throw new FeatureNotImplementedException("Cloud platform not implemented");
    }
  }

  private Optional<DRSAccessMethod> getAccessMethodMatchingAccessId(
      String accessId, DRSObject object, TypeEnum methodType) {
    return object.getAccessMethods().stream()
        .filter(
            drsAccessMethod ->
                drsAccessMethod.getType().equals(methodType)
                    && drsAccessMethod.getAccessId().equals(accessId))
        .findFirst();
  }

  private DRSAccessURL signAzureUrl(
      BillingProfileModel profileModel, FSItem fsItem, AuthenticatedUserRequest authUser) {
    AzureStorageAccountResource storageAccountResource =
        resourceService.lookupStorageAccountMetadata(((FSFile) fsItem).getBucketResourceId());
    return new DRSAccessURL()
        .url(
            azureBlobStorePdao.signFile(
                profileModel,
                storageAccountResource,
                ((FSFile) fsItem).getCloudPath(),
                ContainerType.DATA,
                new BlobSasTokenOptions(
                    URL_TTL,
                    new BlobSasPermission().setReadPermission(true),
                    authUser.getEmail())));
  }

  private DRSAccessURL signGoogleUrl(String googleProjectId, DRSAccessMethod accessMethod) {
    Storage storage =
        StorageOptions.newBuilder().setProjectId(googleProjectId).build().getService();
    String gsPath = accessMethod.getAccessUrl().getUrl();
    BlobId locator = GcsUriUtils.parseBlobUri(gsPath);

    BlobInfo blobInfo = BlobInfo.newBuilder(locator).build();

    URL url =
        storage.signUrl(
            blobInfo,
            URL_TTL.toMinutes(),
            TimeUnit.MINUTES,
            Storage.SignUrlOption.withV4Signature());

    return new DRSAccessURL().url(url.toString());
  }

  private DRSObject drsObjectFromFSFile(
      FSFile fsFile, String snapshotId, AuthenticatedUserRequest authUser) {
    DRSObject fileObject = makeCommonDrsObject(fsFile, snapshotId);

    List<DRSAccessMethod> accessMethods;
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(fsFile.getCloudPlatform());
    if (platform.isGcp()) {
      accessMethods = getDrsAccessMethodsOnGcp(snapshotId, fsFile, authUser);
    } else if (platform.isAzure()) {
      accessMethods = getDrsAccessMethodsOnAzure(fsFile);
    } else {
      throw new InvalidCloudPlatformException();
    }

    fileObject
        .mimeType(fsFile.getMimeType())
        .checksums(fileService.makeChecksums(fsFile))
        .selfUri(drsIdService.makeDrsId(fsFile, snapshotId).toDrsUri())
        .accessMethods(accessMethods);

    return fileObject;
  }

  private List<DRSAccessMethod> getDrsAccessMethodsOnGcp(
      String snapshotId, FSFile fsFile, AuthenticatedUserRequest authUser) {
    DRSAccessURL gsAccessURL = new DRSAccessURL().url(fsFile.getCloudPath());

    SnapshotCacheResult cachedSnapshot = getSnapshot(UUID.fromString(snapshotId));

    final GoogleRegion region;
    if (cachedSnapshot.isSelfHosted) {
      Storage storage = gcsProjectFactory.getStorage(cachedSnapshot.googleProjectId);
      Bucket bucket = storage.get(GcsUriUtils.parseBlobUri(fsFile.getCloudPath()).getBucket());
      region = GoogleRegion.fromValue(bucket.getLocation());
    } else {
      GoogleBucketResource bucketResource =
          resourceService.lookupBucketMetadata(fsFile.getBucketResourceId());
      region = bucketResource.getRegion();
    }

    String accessId = "gcp-" + region.getValue();
    DRSAccessMethod gsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .accessId(accessId)
            .region(region.toString());

    DRSAccessURL httpsAccessURL =
        new DRSAccessURL()
            .url(GcsUriUtils.makeHttpsFromGs(fsFile.getCloudPath()))
            .headers(makeAuthHeader(authUser));

    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(region.toString());

    return List.of(gsAccessMethod, httpsAccessMethod);
  }

  private List<DRSAccessMethod> getDrsAccessMethodsOnAzure(FSFile fsFile) {
    DRSAccessURL accessURL = new DRSAccessURL().url(fsFile.getCloudPath());

    AzureStorageAccountResource storageAccountResource =
        resourceService.lookupStorageAccountMetadata(fsFile.getBucketResourceId());

    AzureRegion region = storageAccountResource.getRegion();
    String accessId = "az-" + region.getValue();
    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(accessURL)
            .accessId(accessId)
            .region(region.toString());

    return List.of(httpsAccessMethod);
  }

  private DRSObject drsObjectFromFSDir(FSDir fsDir, String snapshotId) {
    DRSObject dirObject = makeCommonDrsObject(fsDir, snapshotId);

    DRSChecksum drsChecksum = new DRSChecksum().type("crc32c").checksum("0");
    dirObject.size(0L).addChecksumsItem(drsChecksum).contents(makeContentsList(fsDir, snapshotId));

    return dirObject;
  }

  private DRSObject makeCommonDrsObject(FSItem fsObject, String snapshotId) {
    // Compute the time once; used for both created and updated times as per DRS spec for immutable
    // objects
    String theTime = fsObject.getCreatedDate().toString();
    DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

    return new DRSObject()
        .id(drsId.toDrsObjectId())
        .name(getLastNameFromPath(fsObject.getPath()))
        .createdTime(theTime)
        .updatedTime(theTime)
        .version(DRS_OBJECT_VERSION)
        .description(fsObject.getDescription())
        .aliases(Collections.singletonList(fsObject.getPath()))
        .size(fsObject.getSize())
        .checksums(fileService.makeChecksums(fsObject));
  }

  private List<DRSContentsObject> makeContentsList(FSDir fsDir, String snapshotId) {
    List<DRSContentsObject> contentsList = new ArrayList<>();

    for (FSItem fsObject : fsDir.getContents()) {
      contentsList.add(makeDrsContentsObject(fsObject, snapshotId));
    }

    return contentsList;
  }

  private DRSContentsObject makeDrsContentsObject(FSItem fsObject, String snapshotId) {
    DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

    List<String> drsUris = new ArrayList<>();
    drsUris.add(drsId.toDrsUri());

    DRSContentsObject contentsObject =
        new DRSContentsObject()
            .name(getLastNameFromPath(fsObject.getPath()))
            .id(drsId.toDrsObjectId())
            .drsUri(drsUris);

    if (fsObject instanceof FSDir) {
      FSDir fsDir = (FSDir) fsObject;
      if (fsDir.isEnumerated()) {
        contentsObject.contents(makeContentsList(fsDir, snapshotId));
      }
    }

    return contentsObject;
  }

  private List<String> makeAuthHeader(AuthenticatedUserRequest authUser) {
    // TODO: I added this so that connected tests would work. Seems like we should have a better
    // solution.
    // I don't like putting test-path-only stuff into the production code.
    if (authUser == null || authUser.getToken().isEmpty()) {
      return Collections.emptyList();
    }

    String hdr = String.format("Authorization: Bearer %s", authUser.getToken());
    return Collections.singletonList(hdr);
  }

  public static String getLastNameFromPath(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    return pathParts[pathParts.length - 1];
  }

  private void verifyAuthorization(String snapshotId, AuthenticatedUserRequest authUser) {
    samAuthorizations.computeIfAbsent(
        snapshotId,
        id -> {
          samService.verifyAuthorization(
              authUser, IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
          return id;
        });
  }

  private SnapshotProject getSnapshotProject(UUID snapshotId) {
    return snapshotProjects.computeIfAbsent(
        snapshotId, snapshotService::retrieveAvailableSnapshotProject);
  }

  private SnapshotCacheResult getSnapshot(UUID snapshotId) {
    return snapshotCache.computeIfAbsent(
        snapshotId, id -> new SnapshotCacheResult(snapshotService.retrieve(id)));
  }

  private static class SnapshotCacheResult {
    private final Boolean isSelfHosted;
    private final BillingProfileModel billingProfileModel;
    private String googleProjectId;

    public SnapshotCacheResult(Snapshot snapshot) {
      this.isSelfHosted = snapshot.isSelfHosted();
      this.billingProfileModel =
          snapshot
              .getFirstSnapshotSource()
              .getDataset()
              .getDatasetSummary()
              .getDefaultBillingProfile();

      CloudPlatformWrapper platform =
          CloudPlatformWrapper.of(billingProfileModel.getCloudPlatform());
      if (platform.isGcp()) {
        this.googleProjectId = snapshot.getProjectResource().getGoogleProjectId();
      }
    }
  }
}
