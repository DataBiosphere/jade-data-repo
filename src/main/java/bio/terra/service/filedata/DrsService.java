package bio.terra.service.filedata;

import bio.terra.app.configuration.DrsServiceConfiguration;
import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.model.RASv1Dot1VisaCriterion;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.ras.ECMService;
import bio.terra.service.auth.ras.exception.InvalidAuthorizationMethod;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
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
import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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

  private static final String ACCESS_ID_PREFIX_GCP = "gcp-";
  private static final String ACCESS_ID_PREFIX_AZURE = "az-";
  private static final String ACCESS_ID_PREFIX_PASSPORT = "passport-";
  private static final String RAS_CRITERIA_TYPE = "RASv1Dot1VisaCriterion";
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
  private final ECMService ecmService;
  private final DrsServiceConfiguration drsServiceConfiguration;

  private final Map<UUID, SnapshotProject> snapshotProjects =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<UUID, SnapshotCacheResult> snapshotCache =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<UUID, SnapshotSummaryModel> snapshotSummaries =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));

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
      GcsProjectFactory gcsProjectFactory,
      ECMService ecmService,
      DrsServiceConfiguration drsServiceConfiguration) {
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
    this.ecmService = ecmService;
    this.drsServiceConfiguration = drsServiceConfiguration;
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
   * Determine the acceptable means of authentication for a given DRS ID, including the passport
   * issuers when supported.
   *
   * @param drsObjectId the object ID for which to look up authorizations
   * @return the `DrsAuthorizations` for this ID
   * @throws IllegalArgumentException if there is an issue with the object id
   * @throws SnapshotNotFoundException if the snapshot for the DRS object cannot be found
   * @throws TooManyRequestsException if there are too many concurrent DRS lookup requests
   */
  public DRSAuthorizations lookupAuthorizationsByDrsId(String drsObjectId) {
    try (DrsRequestResource r = new DrsRequestResource()) {
      DRSAuthorizations auths = new DRSAuthorizations();

      auths.addSupportedTypesItem(DRSAuthorizations.SupportedTypesEnum.BEARERAUTH);
      // TODO: add to bearer_auth_issuers, ask Muscles.

      SnapshotCacheResult snapshot = lookupSnapshotForDRSObject(drsObjectId);
      SnapshotSummaryModel snapshotSummary = getSnapshotSummary(snapshot.id);

      String phsId = snapshotSummary.getPhsId();
      String consentCode = snapshotSummary.getConsentCode();

      if (phsId != null && consentCode != null) {
        auths.addSupportedTypesItem(DRSAuthorizations.SupportedTypesEnum.PASSPORTAUTH);
        auths.addPassportAuthIssuersItem(drsServiceConfiguration.getRasIssuer());
      }

      return auths;
    }
  }

  /**
   * Look up the DRS object for a DRS object ID.
   *
   * @param drsObjectId the object ID to look up
   * @param drsPassportRequestModel includes RAS passport, used for authorization and 'expand' var -
   *     if expand is false and drsObjectId refers to a bundle, then the returned array contains
   *     only those objects directly contained in the bundle
   * @return the DRS object for this ID
   * @throws IllegalArgumentException if there is an issue with the object id
   * @throws SnapshotNotFoundException if the snapshot for the DRS object cannot be found
   * @throws TooManyRequestsException if there are too many concurrent DRS lookup requests
   */
  public DRSObject lookupObjectByDrsIdPassport(
      String drsObjectId, DRSPassportRequestModel drsPassportRequestModel) {
    try (DrsRequestResource r = new DrsRequestResource()) {
      SnapshotCacheResult snapshot = lookupSnapshotForDRSObject(drsObjectId);
      verifyPassportAuth(snapshot.id, drsPassportRequestModel);

      return lookupDRSObjectAfterAuth(
          drsPassportRequestModel.isExpand(), snapshot, drsObjectId, null, true);
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
      SnapshotCacheResult cachedSnapshot = lookupSnapshotForDRSObject(drsObjectId);

      String samTimer = performanceLogger.timerStart();
      samService.verifyAuthorization(
          authUser,
          IamResourceType.DATASNAPSHOT,
          cachedSnapshot.id.toString(),
          IamAction.READ_DATA);
      performanceLogger.timerEndAndLog(
          samTimer,
          drsObjectId, // not a flight, so no job id
          this.getClass().getName(),
          "samService.verifyAuthorization");

      return lookupDRSObjectAfterAuth(expand, cachedSnapshot, drsObjectId, authUser, false);
    }
  }

  @VisibleForTesting
  SnapshotCacheResult lookupSnapshotForDRSObject(String drsObjectId) {
    DrsId drsId = drsIdService.fromObjectId(drsObjectId);
    SnapshotCacheResult snapshot;
    try {
      UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
      // We only look up DRS ids for unlocked snapshots.
      String retrieveTimer = performanceLogger.timerStart();

      snapshot = getSnapshot(snapshotId);

      performanceLogger.timerEndAndLog(
          retrieveTimer,
          drsObjectId, // not a flight, so no job id
          this.getClass().getName(),
          "snapshotService.retrieveAvailable");
      return snapshot;
    } catch (IllegalArgumentException ex) {
      throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
    } catch (SnapshotNotFoundException ex) {
      throw new DrsObjectNotFoundException(
          "No snapshot found for DRS object id '" + drsObjectId + "'", ex);
    }
  }

  void verifyPassportAuth(UUID snapshotId, DRSPassportRequestModel drsPassportRequestModel) {
    SnapshotSummaryModel snapshotSummary = getSnapshotSummary(snapshotId);
    String phsId = snapshotSummary.getPhsId();
    String consentCode = snapshotSummary.getConsentCode();
    if (phsId == null || consentCode == null) {
      throw new InvalidAuthorizationMethod("Snapshot cannot use Ras Passport authorization");
    }
    // Pass the passport + phs id + consent code to ECM
    var criteria = new RASv1Dot1VisaCriterion().consentCode(consentCode).phsId(phsId);
    criteria.issuer(drsServiceConfiguration.getRasIssuer()).type(RAS_CRITERIA_TYPE);
    var request =
        new ValidatePassportRequest()
            .passports(drsPassportRequestModel.getPassports())
            .criteria(List.of(criteria));
    var result = ecmService.validatePassport(request);

    var df = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss z");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    logger.info(
        "[Validate Passport Audit]: Data Repository accessed: {}, Study/Data set accessed: {}, Date/Time of access: {}, ECM Audit Info: {}",
        "TDR",
        phsId,
        df.format(new Date(System.currentTimeMillis())),
        result.getAuditInfo());

    if (!result.isValid()) {
      throw new UnauthorizedException("User is not authorized to see drs object.");
    }
  }

  private DRSObject lookupDRSObjectAfterAuth(
      boolean expand,
      SnapshotCacheResult snapshot,
      String drsObjectId,
      AuthenticatedUserRequest authUser,
      boolean passportAuth) {
    DrsId drsId = drsIdService.fromObjectId(drsObjectId);
    SnapshotProject snapshotProject = getSnapshotProject(snapshot.id);
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
      return drsObjectFromFSFile((FSFile) fsObject, snapshot, authUser, passportAuth);
    } else if (fsObject instanceof FSDir) {
      return drsObjectFromFSDir((FSDir) fsObject, drsId.getSnapshotId());
    }

    throw new IllegalArgumentException("Invalid object type");
  }

  public DRSAccessURL postAccessUrlForObjectId(
      String objectId, String accessId, DRSPassportRequestModel passportRequestModel) {
    DRSObject drsObject = lookupObjectByDrsIdPassport(objectId, passportRequestModel);
    return getAccessURL(null, drsObject, objectId, accessId);
  }

  public DRSAccessURL getAccessUrlForObjectId(
      AuthenticatedUserRequest authUser, String objectId, String accessId) {
    DRSObject drsObject = lookupObjectByDrsId(authUser, objectId, false);
    return getAccessURL(authUser, drsObject, objectId, accessId);
  }

  private DRSAccessURL getAccessURL(
      AuthenticatedUserRequest authUser, DRSObject drsObject, String objectId, String accessId) {

    DrsId drsId = drsIdService.fromObjectId(objectId);
    UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
    SnapshotCacheResult cachedSnapshot = getSnapshot(snapshotId);

    BillingProfileModel billingProfileModel = cachedSnapshot.billingProfileModel;

    assertAccessMethodMatchingAccessId(accessId, drsObject);

    FSFile fsFile;
    try {
      fsFile =
          (FSFile)
              fileService.lookupSnapshotFSItem(
                  snapshotService.retrieveAvailableSnapshotProject(cachedSnapshot.id),
                  drsId.getFsObjectId(),
                  1);
    } catch (InterruptedException e) {
      throw new IllegalArgumentException(e);
    }
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(billingProfileModel.getCloudPlatform());
    if (platform.isGcp()) {
      return signGoogleUrl(cachedSnapshot, fsFile.getCloudPath());
    } else if (platform.isAzure()) {
      return signAzureUrl(billingProfileModel, fsFile, authUser);
    } else {
      throw new FeatureNotImplementedException("Cloud platform not implemented");
    }
  }

  private DRSAccessMethod assertAccessMethodMatchingAccessId(String accessId, DRSObject object) {
    Supplier<IllegalArgumentException> illegalArgumentExceptionSupplier =
        () -> new IllegalArgumentException("No matching access ID was found for object");

    return object.getAccessMethods().stream()
        .filter(drsAccessMethod -> drsAccessMethod.getAccessId().equals(accessId))
        .findFirst()
        .orElseThrow(illegalArgumentExceptionSupplier);
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

  private DRSAccessURL signGoogleUrl(SnapshotCacheResult cachedSnapshot, String gsPath) {
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(cachedSnapshot.googleProjectId)
            .build()
            .getService();
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
      FSFile fsFile,
      SnapshotCacheResult cachedSnapshot,
      AuthenticatedUserRequest authUser,
      boolean passportAuth) {
    DRSObject fileObject = makeCommonDrsObject(fsFile, cachedSnapshot.id.toString());

    List<DRSAccessMethod> accessMethods;
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(fsFile.getCloudPlatform());
    if (platform.isGcp()) {
      String gcpRegion = retrieveGCPSnapshotRegion(cachedSnapshot, fsFile);
      if (passportAuth) {
        accessMethods =
            getDrsSignedURLAccessMethods(
                ACCESS_ID_PREFIX_GCP + ACCESS_ID_PREFIX_PASSPORT, gcpRegion);
      } else {
        accessMethods = getDrsAccessMethodsOnGcp(fsFile, authUser, gcpRegion);
      }
    } else if (platform.isAzure()) {
      String azureRegion = retrieveAzureSnapshotRegion(fsFile);
      if (passportAuth) {
        accessMethods =
            getDrsSignedURLAccessMethods(
                ACCESS_ID_PREFIX_AZURE + ACCESS_ID_PREFIX_PASSPORT, azureRegion);
      } else {
        accessMethods = getDrsSignedURLAccessMethods(ACCESS_ID_PREFIX_AZURE, azureRegion);
      }
    } else {
      throw new InvalidCloudPlatformException();
    }

    fileObject
        .mimeType(fsFile.getMimeType())
        .checksums(fileService.makeChecksums(fsFile))
        .selfUri(drsIdService.makeDrsId(fsFile, cachedSnapshot.id.toString()).toDrsUri())
        .accessMethods(accessMethods);

    return fileObject;
  }

  private String retrieveGCPSnapshotRegion(SnapshotCacheResult cachedSnapshot, FSFile fsFile) {
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

    return region.getValue();
  }

  private String retrieveAzureSnapshotRegion(FSFile fsFile) {
    AzureStorageAccountResource storageAccountResource =
        resourceService.lookupStorageAccountMetadata(fsFile.getBucketResourceId());

    return storageAccountResource.getRegion().getValue();
  }

  private List<DRSAccessMethod> getDrsAccessMethodsOnGcp(
      FSFile fsFile, AuthenticatedUserRequest authUser, String region) {
    DRSAccessURL gsAccessURL = new DRSAccessURL().url(fsFile.getCloudPath());

    String accessId = ACCESS_ID_PREFIX_GCP + region;
    DRSAccessMethod gsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .accessId(accessId)
            .region(region);

    DRSAccessURL httpsAccessURL =
        new DRSAccessURL()
            .url(GcsUriUtils.makeHttpsFromGs(fsFile.getCloudPath()))
            .headers(makeAuthHeader(authUser));

    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(region);

    return List.of(gsAccessMethod, httpsAccessMethod);
  }

  private List<DRSAccessMethod> getDrsSignedURLAccessMethods(String prefix, String region) {
    String accessId = prefix + region;
    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessId(accessId)
            .region(region);

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

  private SnapshotProject getSnapshotProject(UUID snapshotId) {
    return snapshotProjects.computeIfAbsent(
        snapshotId, snapshotService::retrieveAvailableSnapshotProject);
  }

  private SnapshotCacheResult getSnapshot(UUID snapshotId) {
    return snapshotCache.computeIfAbsent(
        snapshotId, id -> new SnapshotCacheResult(snapshotService.retrieve(id)));
  }

  private SnapshotSummaryModel getSnapshotSummary(UUID snapshotId) {
    return snapshotSummaries.computeIfAbsent(snapshotId, snapshotService::retrieveSnapshotSummary);
  }

  @VisibleForTesting
  static class SnapshotCacheResult {
    private final UUID id;
    private final Boolean isSelfHosted;
    private final BillingProfileModel billingProfileModel;
    private final String googleProjectId;

    public SnapshotCacheResult(Snapshot snapshot) {
      this.id = snapshot.getId();
      this.isSelfHosted = snapshot.isSelfHosted();
      this.billingProfileModel =
          snapshot.getSourceDataset().getDatasetSummary().getDefaultBillingProfile();
      var projectResource = snapshot.getProjectResource();
      if (projectResource != null) {
        this.googleProjectId = projectResource.getGoogleProjectId();
      } else {
        this.googleProjectId = null;
      }
    }

    public UUID getId() {
      return this.id;
    }
  }
}
