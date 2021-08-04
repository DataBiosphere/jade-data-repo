package bio.terra.service.filedata;

import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.NotImplementedException;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessMethod.TypeEnum;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao.GcsLocator;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import com.azure.storage.blob.BlobUrlParts;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final Logger logger = LoggerFactory.getLogger("bio.terra.service.filedata.DrsService");

  private static final String DRS_OBJECT_VERSION = "0";
  private static final int URL_TTL = 15;
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
  private final ProfileService profileService;
  private final AzureResourceConfiguration resourceConfiguration;

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
      ProfileService profileService,
      AzureResourceConfiguration resourceConfiguration) {
    this.snapshotService = snapshotService;
    this.fileService = fileService;
    this.drsIdService = drsIdService;
    this.samService = samService;
    this.resourceService = resourceService;
    this.configurationService = configurationService;
    this.jobService = jobService;
    this.performanceLogger = performanceLogger;
    this.profileService = profileService;
    this.resourceConfiguration = resourceConfiguration;
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
      SnapshotProject snapshotProject = null;
      try {
        UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
        // We only look up DRS ids for unlocked snapshots.
        String retrieveTimer = performanceLogger.timerStart();

        snapshotProject = snapshotService.retrieveAvailableSnapshotProject(snapshotId);

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

      samService.verifyAuthorization(
          authUser, IamResourceType.DATASNAPSHOT, drsId.getSnapshotId(), IamAction.READ_DATA);

      performanceLogger.timerEndAndLog(
          samTimer,
          drsObjectId, // not a flight, so no job id
          this.getClass().getName(),
          "samService.verifyAuthorization");

      int depth = (expand ? -1 : 1);

      FSItem fsObject = null;
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
    Snapshot snapshot = snapshotService.retrieve(UUID.fromString(drsId.getSnapshotId()));

    BillingProfileModel billingProfileModel =
        snapshot
            .getFirstSnapshotSource()
            .getDataset()
            .getDatasetSummary()
            .getDefaultBillingProfile();

    CloudPlatformWrapper wrapper = CloudPlatformWrapper.of(billingProfileModel.getCloudPlatform());
    if (wrapper.isGcp()) {
      DRSAccessMethod matchingAccessMethod =
          getAccessMethodMatchingAccessId(accessId, drsObject, TypeEnum.GS)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "No matching access ID "
                              + accessId
                              + " was found on object "
                              + objectId));
      return signGoogleUrl(snapshot, matchingAccessMethod);
    } else if (wrapper.isAzure()) {
      DRSAccessMethod matchingAccessMethod =
          getAccessMethodMatchingAccessId(accessId, drsObject, TypeEnum.HTTPS)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "No matching access ID "
                              + accessId
                              + " was found on object "
                              + objectId));
      return signAzureUrl(billingProfileModel, snapshot, drsId);
    } else {
      throw new NotImplementedException("Cloud platform not implemented");
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
      BillingProfileModel profileModel, Snapshot snapshot, DrsId drsId) {
    FSItem fsObject = null;
    try {
      fsObject =
          fileService.lookupSnapshotFSItem(
              snapshotService.retrieveAvailableSnapshotProject(snapshot.getId()),
              drsId.getFsObjectId(),
              1);

      BlobUrlParts blobUrl = BlobUrlParts.parse(fsObject.getPath());
      BlobContainerClientFactory sourceClientFactory =
          new BlobContainerClientFactory(
              blobUrl.getAccountName(),
              resourceConfiguration.getAppToken(profileModel.getTenantId()),
              blobUrl.getBlobContainerName());

      return new DRSAccessURL()
          .url(sourceClientFactory.createReadOnlySasUrlForBlob(blobUrl.getBlobName()));
    } catch (InterruptedException e) {
      throw new FileSystemExecutionException(
          "Unexpected interruption during file system processing", e);
    }
  }

  private DRSAccessURL signGoogleUrl(Snapshot snapshot, DRSAccessMethod accessMethod) {
    GoogleProjectResource projectResource = snapshot.getProjectResource();
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(projectResource.getGoogleProjectId())
            .build()
            .getService();
    String gsPath = accessMethod.getAccessUrl().getUrl();
    GcsLocator locator = GcsPdao.getGcsLocatorFromGsPath(gsPath);

    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(locator.getBucket(), locator.getPath())).build();

    URL url =
        storage.signUrl(
            blobInfo, URL_TTL, TimeUnit.MINUTES, Storage.SignUrlOption.withV4Signature());

    return new DRSAccessURL().url(url.toString());
  }

  private DRSObject drsObjectFromFSFile(
      FSFile fsFile, String snapshotId, AuthenticatedUserRequest authUser) {
    DRSObject fileObject = makeCommonDrsObject(fsFile, snapshotId);

    List<DRSAccessMethod> accessMethods;
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(fsFile.getCloudPlatform());
    if (platformWrapper.isGcp()) {
      accessMethods = getDrsAccessMethodsOnGcp(fsFile, authUser);
    } else if (platformWrapper.isAzure()) {
      accessMethods = getDrsAccessMethodsOnAzure(fsFile);
    } else {
      throw new IllegalArgumentException("Unrecognized cloud platform");
    }

    fileObject
        .mimeType(fsFile.getMimeType())
        .checksums(fileService.makeChecksums(fsFile))
        .accessMethods(accessMethods);

    return fileObject;
  }

  private List<DRSAccessMethod> getDrsAccessMethodsOnGcp(
      FSFile fsFile, AuthenticatedUserRequest authUser) {
    DRSAccessURL gsAccessURL = new DRSAccessURL().url(fsFile.getGspath());

    GoogleBucketResource bucketResource =
        resourceService.lookupBucketMetadata(fsFile.getBucketResourceId());

    GoogleRegion region = bucketResource.getRegion();
    String accessId = "gcp-" + region.getValue();
    DRSAccessMethod gsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .accessId(accessId)
            .region(bucketResource.getRegion().toString());

    DRSAccessURL httpsAccessURL =
        new DRSAccessURL()
            .url(makeHttpsFromGs(fsFile.getGspath()))
            .headers(makeAuthHeader(authUser));

    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(bucketResource.getRegion().toString());

    return List.of(gsAccessMethod, httpsAccessMethod);
  }

  private List<DRSAccessMethod> getDrsAccessMethodsOnAzure(FSFile fsFile) {
    DRSAccessURL accessURL = new DRSAccessURL().url(fsFile.getGspath());

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

  private String makeHttpsFromGs(String gspath) {
    try {
      GcsPdao.GcsLocator locator = GcsPdao.getGcsLocatorFromGsPath(gspath);
      String gsBucket = locator.getBucket();
      String gsPath = locator.getPath();
      String encodedPath =
          URLEncoder.encode(gsPath, StandardCharsets.UTF_8.toString())
              // Google does not recognize the + characters that are produced from spaces by the
              // URLEncoder.encode
              // method. As a result, these must be converted to %2B.
              .replaceAll("\\+", "%20");
      return String.format(
          "https://www.googleapis.com/storage/v1/b/%s/o/%s?alt=media", gsBucket, encodedPath);
    } catch (UnsupportedEncodingException ex) {
      throw new InvalidDrsIdException("Failed to urlencode file path", ex);
    }
  }

  private List<String> makeAuthHeader(AuthenticatedUserRequest authUser) {
    // TODO: I added this so that connected tests would work. Seems like we should have a better
    // solution.
    // I don't like putting test-path-only stuff into the production code.
    if (authUser == null || !authUser.getToken().isPresent()) {
      return Collections.EMPTY_LIST;
    }

    String hdr = String.format("Authorization: Bearer %s", authUser.getRequiredToken());
    return Collections.singletonList(hdr);
  }

  public static String getLastNameFromPath(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    return pathParts[pathParts.length - 1];
  }
}
