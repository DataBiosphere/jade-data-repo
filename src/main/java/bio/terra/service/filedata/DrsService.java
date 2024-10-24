package bio.terra.service.filedata;

import static bio.terra.service.filedata.google.gcs.GcsConstants.REQUESTED_BY_QUERY_PARAM;
import static bio.terra.service.filedata.google.gcs.GcsConstants.USER_PROJECT_QUERY_PARAM;

import bio.terra.app.configuration.DrsConfiguration;
import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.GoogleRegion;
import bio.terra.app.usermetrics.BardEventProperties;
import bio.terra.app.usermetrics.UserLoggingMetrics;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.FutureUtils;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.model.DrsAliasModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.admin.flight.DrsAliasRegisterFlight;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.DrsDao.DrsAlias;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.GoogleInternalServerErrorException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.exception.InvalidDrsObjectException;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
@Component
public class DrsService {

  private static final String ACCESS_ID_PREFIX_GCP = "gcp-";
  private static final String ACCESS_ID_PREFIX_AZURE = "az-";
  private static final String ACCESS_ID_PREFIX_PASSPORT = "passport-";
  private static final String ACCESS_ID_SEPARATOR = "*";
  private static final String DRS_OBJECT_VERSION = "0";
  @VisibleForTesting static final Duration URL_TTL = Duration.ofMinutes(15);

  private final SnapshotService snapshotService;
  private final FileService fileService;
  private final DrsIdService drsIdService;
  private final IamService samService;
  private final ResourceService resourceService;
  private final DrsConfiguration drsConfiguration;
  private final JobService jobService;
  private final PerformanceLogger performanceLogger;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final GcsProjectFactory gcsProjectFactory;
  private final EcmConfiguration ecmConfiguration;
  private final DrsDao drsDao;
  private final DrsMetricsService drsMetricsService;
  private final AsyncTaskExecutor executor;
  private final UserLoggingMetrics loggingMetrics;

  private final Map<UUID, SnapshotProject> snapshotProjectsCache =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<UUID, SnapshotCacheResult> snapshotCache =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  private final Map<UUID, SnapshotSummaryModel> snapshotSummariesCache =
      Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));

  public DrsService(
      SnapshotService snapshotService,
      FileService fileService,
      DrsIdService drsIdService,
      IamService samService,
      ResourceService resourceService,
      DrsConfiguration drsConfiguration,
      JobService jobService,
      PerformanceLogger performanceLogger,
      AzureBlobStorePdao azureBlobStorePdao,
      GcsProjectFactory gcsProjectFactory,
      EcmConfiguration ecmConfiguration,
      DrsDao drsDao,
      DrsMetricsService drsMetricsService,
      @Qualifier("drsResolutionThreadpool") AsyncTaskExecutor executor,
      UserLoggingMetrics loggingMetrics) {
    this.snapshotService = snapshotService;
    this.fileService = fileService;
    this.drsIdService = drsIdService;
    this.samService = samService;
    this.resourceService = resourceService;
    this.drsConfiguration = drsConfiguration;
    this.jobService = jobService;
    this.performanceLogger = performanceLogger;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.gcsProjectFactory = gcsProjectFactory;
    this.ecmConfiguration = ecmConfiguration;
    this.drsDao = drsDao;
    this.drsMetricsService = drsMetricsService;
    this.executor = executor;
    this.loggingMetrics = loggingMetrics;
  }

  private class DrsRequestResource implements AutoCloseable {

    DrsRequestResource() {
      int podCount = jobService.getActivePodCount();
      drsMetricsService.setDrsRequestMax(drsConfiguration.maxDrsLookups() / podCount);
      drsMetricsService.tryIncrementCurrentDrsRequestCount();
    }

    @Override
    public void close() {
      drsMetricsService.decrementCurrentDrsRequestCount();
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
      DrsId resolvedDrsObjectId = resolveDrsObjectId(drsObjectId);
      List<SnapshotCacheResult> snapshots = lookupSnapshotsForDRSObject(resolvedDrsObjectId);
      List<SnapshotSummaryModel> snapshotSummaries =
          snapshots.stream().map(SnapshotCacheResult::id).map(this::getSnapshotSummary).toList();

      return buildDRSAuth(
          snapshotSummaries.stream().anyMatch(SnapshotSummary::passportAuthorizationAvailable));
    }
  }

  private DRSAuthorizations buildDRSAuth(boolean passportAuthorizationAvailable) {
    DRSAuthorizations auths = new DRSAuthorizations();
    if (passportAuthorizationAvailable) {
      auths.addSupportedTypesItem(DRSAuthorizations.SupportedTypesEnum.PASSPORTAUTH);
      auths.addPassportAuthIssuersItem(ecmConfiguration.rasIssuer());
    }
    auths.addSupportedTypesItem(DRSAuthorizations.SupportedTypesEnum.BEARERAUTH);
    return auths;
  }

  public long recordDrsIdToSnapshot(UUID snapshotId, List<DrsId> drsIds) {
    return drsDao.recordDrsIdToSnapshot(snapshotId, drsIds);
  }

  public long deleteDrsIdToSnapshotsBySnapshot(UUID snapshotId) {
    return drsDao.deleteDrsIdToSnapshotsBySnapshot(snapshotId);
  }

  private List<UUID> retrieveReferencedSnapshotIds(DrsId drsId) {
    return drsDao.retrieveReferencedSnapshotIds(drsId);
  }

  private DrsId retrieveDrsAliasByAlias(String alias) {
    return Optional.ofNullable(drsDao.retrieveDrsAliasByAlias(alias))
        .map(DrsAlias::tdrDrsObjectId)
        .orElse(null);
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
      DrsId resolvedDrsObjectId = resolveDrsObjectId(drsObjectId);
      List<Future<SnapshotCacheResult>> futures =
          lookupSnapshotsForDRSObject(resolvedDrsObjectId).stream()
              .map(
                  s ->
                      executor.submit(
                          () -> {
                            try {
                              // Only look at snapshots that the user has access to
                              verifyPassportAuth(s.id, drsPassportRequestModel);
                              return s;
                            } catch (UnauthorizedException e) {
                              return null;
                            }
                          }))
              .toList();
      List<SnapshotCacheResult> cachedSnapshots = FutureUtils.waitFor(futures);
      if (cachedSnapshots.isEmpty()) {
        throw new UnauthorizedException("User does not have access");
      }
      return resolveDRSObject(
          null, resolvedDrsObjectId, drsPassportRequestModel.isExpand(), cachedSnapshots, true);
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
      AuthenticatedUserRequest authUser, String drsObjectId, boolean expand) {
    try (DrsRequestResource r = new DrsRequestResource()) {
      DrsId resolvedDrsObjectId = resolveDrsObjectId(drsObjectId);
      String samTimer = performanceLogger.timerStart();
      List<Future<SnapshotCacheResult>> futures =
          lookupSnapshotsForDRSObject(resolvedDrsObjectId).stream()
              .map(
                  s ->
                      executor.submit(
                          () -> {
                            try {
                              // Only look at snapshots that the user has access to
                              samService.verifyAuthorization(
                                  authUser,
                                  IamResourceType.DATASNAPSHOT,
                                  s.id.toString(),
                                  IamAction.READ_DATA);
                              return s;
                            } catch (IamForbiddenException e) {
                              return null;
                            }
                          }))
              .toList();
      List<SnapshotCacheResult> cachedSnapshots = FutureUtils.waitFor(futures);
      if (cachedSnapshots.isEmpty()) {
        throw new IamForbiddenException("User does not have access");
      }
      performanceLogger.timerEndAndLog(
          samTimer,
          drsObjectId, // not a flight, so no job id
          this.getClass().getName(),
          "samService.verifyAuthorization");

      return resolveDRSObject(authUser, resolvedDrsObjectId, expand, cachedSnapshots, false);
    }
  }

  /**
   * Given the precalculated list of associated snapshots, look up the DRS object in the various
   * firestore/azure table dbs and merged into a single DRSObject. Note: this will fail if object
   * overlap in invalid ways, such as mismatched checksums, multiple names, etc.
   */
  private DRSObject resolveDRSObject(
      AuthenticatedUserRequest authUser,
      DrsId drsId,
      boolean expand,
      List<SnapshotCacheResult> cachedSnapshots,
      boolean passportAuth) {

    Map<UUID, UUID> snapshotToBillingSnapshot = chooseBillingSnapshotsPerSnapshot(cachedSnapshots);
    List<Future<DRSObject>> futures =
        cachedSnapshots.stream()
            .map(
                s ->
                    executor.submit(
                        () ->
                            lookupDRSObjectAfterAuth(
                                expand,
                                s,
                                drsId,
                                authUser,
                                passportAuth,
                                snapshotToBillingSnapshot.get(s.id).toString())))
            .toList();
    List<DRSObject> drsObjects = FutureUtils.waitFor(futures);

    return mergeDRSObjects(drsObjects);
  }

  /**
   * Given a list of snapshots, return a map of snapshot ids mapped to the snapshot to use for
   * billing
   */
  @VisibleForTesting
  Map<UUID, UUID> chooseBillingSnapshotsPerSnapshot(List<SnapshotCacheResult> cachedSnapshots) {
    // First create a multimap keyed on billing profile id whose values are a list of snapshots
    // sorted by id
    Map<UUID, List<SnapshotCacheResult>> snapshotsByBillingId =
        cachedSnapshots.stream()
            .collect(Collectors.groupingBy(SnapshotCacheResult::snapshotBillingProfileId))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .sorted(Comparator.comparing(SnapshotCacheResult::id))
                            .toList()));

    // Create the map keyed on each snapshot id and whose value is the first snapshot for that
    // snapshot's group (when grouped by billing account)
    record BillingAndSnapshot(UUID billingId, SnapshotCacheResult snapshot) {}
    return snapshotsByBillingId.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(v -> new BillingAndSnapshot(e.getKey(), v)))
        .collect(
            Collectors.toMap(
                e -> e.snapshot().id(), e -> snapshotsByBillingId.get(e.billingId()).get(0).id()));
  }

  @VisibleForTesting
  List<SnapshotCacheResult> lookupSnapshotsForDRSObject(DrsId drsId) {
    try {
      List<UUID> snapshotIds = new ArrayList<>();
      if (drsId.getVersion().equals("v1")) {
        snapshotIds.add(UUID.fromString(drsId.getSnapshotId()));
      } else if (drsId.getVersion().equals("v2")) {
        snapshotIds.addAll(retrieveReferencedSnapshotIds(drsId));
      } else {
        throw new InvalidDrsIdException("Invalid DRS ID version %s".formatted(drsId.getVersion()));
      }
      String retrieveTimer = performanceLogger.timerStart();

      List<SnapshotCacheResult> snapshots =
          snapshotIds.stream()
              .map(
                  i -> {
                    try {
                      return this.getSnapshot(i);
                    } catch (SnapshotNotFoundException ex) {
                      return null;
                    }
                  })
              .filter(Objects::nonNull)
              .toList();

      performanceLogger.timerEndAndLog(
          retrieveTimer,
          drsId.toDrsObjectId(), // not a flight, so no job id
          this.getClass().getName(),
          "snapshotService.retrieveAvailable");
      if (snapshots.isEmpty()) {
        throw new DrsObjectNotFoundException("No snapshots found for this DRS Object ID");
      }
      return snapshots;
    } catch (IllegalArgumentException ex) {
      throw new InvalidDrsIdException(
          "Invalid object id format '%s'".formatted(drsId.toDrsObjectId()), ex);
    }
  }

  void verifyPassportAuth(UUID snapshotId, DRSPassportRequestModel drsPassportRequestModel) {
    SnapshotSummaryModel snapshotSummary = getSnapshotSummary(snapshotId);
    List<String> passports = drsPassportRequestModel.getPassports();
    if (!snapshotService.verifyPassportAuth(snapshotSummary, passports).isValid()) {
      throw new UnauthorizedException("User is not authorized to see drs object.");
    }
  }

  private DRSObject lookupDRSObjectAfterAuth(
      boolean expand,
      SnapshotCacheResult snapshot,
      DrsId drsId,
      AuthenticatedUserRequest authUser,
      boolean passportAuth,
      String billingSnapshot) {
    SnapshotProject snapshotProject = getSnapshotProject(snapshot.id);
    int depth = (expand ? -1 : 1);

    FSItem fsObject;
    try {
      String lookupTimer = performanceLogger.timerStart();
      fsObject = fileService.lookupSnapshotFSItem(snapshotProject, drsId.getFsObjectId(), depth);

      performanceLogger.timerEndAndLog(
          lookupTimer,
          drsId.toDrsObjectId(), // not a flight, so no job id
          this.getClass().getName(),
          "fileService.lookupSnapshotFSItem");
    } catch (InterruptedException ex) {
      throw new FileSystemExecutionException(
          "Unexpected interruption during file system processing", ex);
    }

    if (fsObject instanceof FSFile fsFile) {
      return drsObjectFromFSFile(fsFile, snapshot, authUser, passportAuth, billingSnapshot);
    } else if (fsObject instanceof FSDir fsDir) {
      return drsObjectFromFSDir(fsDir, snapshot);
    }

    throw new IllegalArgumentException("Invalid object type");
  }

  public DRSAccessURL postAccessUrlForObjectId(
      String objectId,
      String accessId,
      DRSPassportRequestModel passportRequestModel,
      String userProject) {
    DRSObject drsObject = lookupObjectByDrsIdPassport(objectId, passportRequestModel);
    return getAccessURL(null, drsObject, accessId, userProject);
  }

  public DRSAccessURL getAccessUrlForObjectId(
      AuthenticatedUserRequest authUser, String objectId, String accessId, String userProject) {
    DRSObject drsObject = lookupObjectByDrsId(authUser, objectId, false);
    return getAccessURL(authUser, drsObject, accessId, userProject);
  }

  private DRSAccessURL getAccessURL(
      AuthenticatedUserRequest authUser, DRSObject drsObject, String accessId, String userProject) {
    // To avoid having to re-resolve the DRS ID in case it is an alias, use the id from the passed
    // in DRS object.
    DrsId drsId = drsIdService.fromObjectId(drsObject.getId());

    UUID snapshotId;
    if (drsId.getSnapshotId() != null) {
      snapshotId = UUID.fromString(drsId.getSnapshotId());
    } else {
      String[] parts = accessId.split("\\Q" + ACCESS_ID_SEPARATOR + "\\E");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid access id");
      }
      snapshotId = UUID.fromString(parts[1]);
    }
    SnapshotCacheResult cachedSnapshot = getSnapshot(snapshotId);

    BillingProfileModel billingProfileModel = cachedSnapshot.datasetBillingProfileModel;

    logAdditionalProperties(cachedSnapshot);

    assertAccessMethodMatchingAccessId(accessId, drsObject);

    FSFile fsFile;
    try {
      fsFile =
          (FSFile)
              fileService.lookupSnapshotFSItem(
                  snapshotService.retrieveSnapshotProject(cachedSnapshot.id),
                  drsId.getFsObjectId(),
                  1);
    } catch (InterruptedException e) {
      throw new IllegalArgumentException(e);
    }

    CloudPlatformWrapper platform = CloudPlatformWrapper.of(cachedSnapshot.cloudPlatform);
    if (platform.isGcp()) {
      return signGoogleUrl(cachedSnapshot, fsFile.getCloudPath(), authUser, userProject);
    } else if (platform.isAzure()) {
      return signAzureUrl(billingProfileModel, fsFile, authUser);
    } else {
      throw new FeatureNotImplementedException("Cloud platform not implemented");
    }
  }

  /**
   * Include additional properties in the event log sent to Bard
   *
   * @param cachedSnapshot
   */
  private void logAdditionalProperties(SnapshotCacheResult cachedSnapshot) {
    loggingMetrics.set(BardEventProperties.DATASET_ID_FIELD_NAME, cachedSnapshot.datasetId());
    loggingMetrics.set(BardEventProperties.DATASET_NAME_FIELD_NAME, cachedSnapshot.datasetName());
    loggingMetrics.set(BardEventProperties.SNAPSHOT_ID_FIELD_NAME, cachedSnapshot.id());
    loggingMetrics.set(BardEventProperties.SNAPSHOT_NAME_FIELD_NAME, cachedSnapshot.name());
    loggingMetrics.set(
        BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME,
        cachedSnapshot.snapshotBillingProfileId());
    loggingMetrics.set(
        BardEventProperties.CLOUD_PLATFORM_FIELD_NAME, cachedSnapshot.cloudPlatform());
  }

  @VisibleForTesting
  DrsId resolveDrsObjectId(String drsObjectId) {
    // If the drsObjectId is not a TDR generated DRS ID, then assume it's an alias and resolve it
    // to a TDR DRS ID
    DrsId drsId;
    if (!drsIdService.isValidObjectId(drsObjectId)) {
      drsId = retrieveDrsAliasByAlias(drsObjectId);
      if (drsId == null) {
        throw new InvalidDrsIdException("Invalid DRS ID %s".formatted(drsObjectId));
      }
    } else {
      drsId = drsIdService.fromObjectId(drsObjectId);
    }
    return drsId;
  }

  private DRSAccessMethod assertAccessMethodMatchingAccessId(String accessId, DRSObject object) {
    Supplier<IllegalArgumentException> illegalArgumentExceptionSupplier =
        () -> new IllegalArgumentException("No matching access ID was found for object");

    return object.getAccessMethods().stream()
        .filter(drsAccessMethod -> Objects.equals(drsAccessMethod.getAccessId(), accessId))
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
                new BlobSasTokenOptions(
                    URL_TTL,
                    new BlobSasPermission().setReadPermission(true),
                    authUser.getEmail())));
  }

  private DRSAccessURL signGoogleUrl(
      SnapshotCacheResult cachedSnapshot,
      String gsPath,
      AuthenticatedUserRequest authUser,
      String userProject) {
    BlobId locator = GcsUriUtils.parseBlobUri(gsPath);

    BlobInfo blobInfo = BlobInfo.newBuilder(locator).build();

    Function<Storage, URL> signUrlFunction =
        storage ->
            storage.signUrl(
                blobInfo,
                URL_TTL.toMinutes(),
                TimeUnit.MINUTES,
                getUrlSigningOptions(cachedSnapshot, userProject, authUser));

    final URL signedUrl;
    if (cachedSnapshot.isSelfHosted) {
      // If a userProject is explicitly passed in, then use that to sign the url.
      // Note: the expectation is that this is a Terra hosted bucket
      if (!StringUtils.isEmpty(userProject)) {
        return new DRSAccessURL()
            .url(samService.signUrlForBlob(authUser, userProject, gsPath, URL_TTL));
      }
      // In the base case of a self-hosted dataset, use the dataset's service account to sign the
      // url
      signedUrl =
          signUrlFunction.apply(gcsProjectFactory.getStorage(cachedSnapshot.datasetProjectId));
    } else {
      try (Storage storage = initStorage(cachedSnapshot.googleProjectId)) {
        signedUrl = signUrlFunction.apply(storage);
      } catch (Exception e) {
        throw new GoogleInternalServerErrorException("Error getting storage ", e);
      }
    }

    return new DRSAccessURL().url(signedUrl.toString());
  }

  @VisibleForTesting
  Storage initStorage(String projectId) {
    return StorageOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  /**
   * Returns the options to use when signing a URL from TDR
   *
   * @param cachedSnapshot Snapshot where the drs request initiated
   * @param userProject The Google project being billed for the accessing the signed URL
   * @param authUser The user requesting the signed URL
   * @return The options to use when signing a URL from TDR
   */
  private Storage.SignUrlOption[] getUrlSigningOptions(
      SnapshotCacheResult cachedSnapshot, String userProject, AuthenticatedUserRequest authUser) {
    final String signingProject;
    final String signingUser;
    // If a user specifies a billing project in the request, prefer that over the snapshot's
    // project.  If a billing project is required to access the data, and it is not provided,
    // nothing will fail until the user tries to access the signed URL.
    if (!StringUtils.isEmpty(userProject)) {
      signingProject = userProject;
    } else {
      signingProject = cachedSnapshot.googleProjectId;
    }
    if (authUser != null) {
      signingUser = authUser.getEmail();
    } else {
      signingUser = null;
    }

    Map<String, String> queryParams = new HashMap<>();
    queryParams.put(USER_PROJECT_QUERY_PARAM, signingProject);
    if (signingUser != null) {
      queryParams.put(REQUESTED_BY_QUERY_PARAM, signingUser);
    }
    return new Storage.SignUrlOption[] {
      Storage.SignUrlOption.withQueryParams(queryParams), Storage.SignUrlOption.withV4Signature()
    };
  }

  private DRSObject drsObjectFromFSFile(
      FSFile fsFile,
      SnapshotCacheResult cachedSnapshot,
      AuthenticatedUserRequest authUser,
      boolean passportAuth,
      String billingSnapshot) {
    DRSObject fileObject = makeCommonDrsObject(fsFile, cachedSnapshot);

    List<DRSAccessMethod> accessMethods;
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(fsFile.getCloudPlatform());
    if (platform.isGcp()) {
      String gcpRegion = retrieveGCPSnapshotRegion(cachedSnapshot, fsFile);
      if (passportAuth) {
        accessMethods =
            getDrsSignedURLAccessMethods(
                ACCESS_ID_PREFIX_GCP + ACCESS_ID_PREFIX_PASSPORT,
                gcpRegion,
                passportAuth,
                billingSnapshot);
      } else {
        accessMethods =
            getDrsAccessMethodsOnGcp(
                fsFile, authUser, gcpRegion, cachedSnapshot.googleProjectId, billingSnapshot);
      }
    } else if (platform.isAzure()) {
      String azureRegion = retrieveAzureSnapshotRegion(fsFile);
      if (passportAuth) {
        accessMethods =
            getDrsSignedURLAccessMethods(
                ACCESS_ID_PREFIX_AZURE + ACCESS_ID_PREFIX_PASSPORT,
                azureRegion,
                passportAuth,
                cachedSnapshot.globalFileIds ? billingSnapshot : null);
      } else {
        accessMethods =
            getDrsSignedURLAccessMethods(
                ACCESS_ID_PREFIX_AZURE,
                azureRegion,
                passportAuth,
                cachedSnapshot.globalFileIds ? billingSnapshot : null);
      }
    } else {
      throw new InvalidCloudPlatformException();
    }

    fileObject
        .mimeType(fsFile.getMimeType())
        .checksums(FileService.makeChecksums(fsFile))
        .accessMethods(accessMethods);

    return fileObject;
  }

  /**
   * @param cachedSnapshot a cached snapshot entry for a GCP-backed snapshot
   * @param fsFile a file entry for a file within the snapshot specifying a Google Cloud Storage URI
   * @return the GCP region associated with the GCS URI, with lookup billed to the snapshot's
   *     project if self-hosted
   * @throws DrsObjectNotFoundException if the snapshot is self-hosted and TDR cannot retrieve the
   *     bucket specified by the GCS URI
   */
  private String retrieveGCPSnapshotRegion(SnapshotCacheResult cachedSnapshot, FSFile fsFile)
      throws DrsObjectNotFoundException {
    final GoogleRegion region;
    if (cachedSnapshot.isSelfHosted) {
      // Authorize using the dataset's service account...
      Storage storage = gcsProjectFactory.getStorage(cachedSnapshot.datasetProjectId);
      // ...but bill to the snapshot's project (available for all GCP-backed snapshots, regardless
      // of their self-hosted status).
      BucketGetOption userProject = BucketGetOption.userProject(cachedSnapshot.googleProjectId);
      String cloudPath = fsFile.getCloudPath();
      Bucket bucket = storage.get(GcsUriUtils.parseBlobUri(cloudPath).getBucket(), userProject);
      if (bucket == null) {
        throw new DrsObjectNotFoundException(
            """
                Could not access GCS URI %s found in self-hosted GCP snapshot.
                Source snapshot: %s
                File entry: %s
                """
                .formatted(cloudPath, cachedSnapshot, fsFile));
      }
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
      FSFile fsFile,
      AuthenticatedUserRequest authUser,
      String region,
      String userProject,
      String billingSnapshot) {
    DRSAccessURL gsAccessURL = new DRSAccessURL().url(fsFile.getCloudPath());
    DRSAuthorizations authorizationsBearerOnly = buildDRSAuth(false);

    String accessId =
        ACCESS_ID_PREFIX_GCP
            + region
            + Optional.ofNullable(billingSnapshot).map(s -> ACCESS_ID_SEPARATOR + s).orElse("");
    DRSAccessMethod gsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .accessId(accessId)
            .region(region)
            .authorizations(authorizationsBearerOnly);

    DRSAccessURL httpsAccessURL =
        new DRSAccessURL()
            .url(GcsUriUtils.makeHttpsFromGs(fsFile.getCloudPath(), userProject))
            .headers(makeAuthHeader(authUser));

    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(region)
            .authorizations(authorizationsBearerOnly);

    return List.of(gsAccessMethod, httpsAccessMethod);
  }

  private List<DRSAccessMethod> getDrsSignedURLAccessMethods(
      String prefix, String region, boolean passportAuth, String billingProject) {
    DRSAuthorizations authorizations = buildDRSAuth(passportAuth);
    String accessId =
        prefix
            + region
            + Optional.ofNullable(billingProject).map(b -> ACCESS_ID_SEPARATOR + b).orElse("");
    DRSAccessMethod httpsAccessMethod =
        new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessId(accessId)
            .region(region)
            .authorizations(authorizations);

    return List.of(httpsAccessMethod);
  }

  private DRSObject drsObjectFromFSDir(FSDir fsDir, SnapshotCacheResult snapshot) {
    return makeCommonDrsObject(fsDir, snapshot).contents(makeContentsList(fsDir, snapshot));
  }

  private DRSObject makeCommonDrsObject(FSItem fsObject, SnapshotCacheResult snapshot) {
    // Compute the time once; used for both created and updated times as per DRS spec for immutable
    // objects
    String theTime = fsObject.getCreatedDate().toString();
    DrsId drsId;
    if (snapshot.globalFileIds) {
      drsId = drsIdService.makeDrsId(fsObject);
    } else {
      drsId = drsIdService.makeDrsId(fsObject, snapshot.id.toString());
    }

    return new DRSObject()
        .id(drsId.toDrsObjectId())
        .selfUri(drsId.toDrsUri())
        .name(getLastNameFromPath(fsObject.getPath()))
        .createdTime(theTime)
        .updatedTime(theTime)
        .version(DRS_OBJECT_VERSION)
        .description(fsObject.getDescription())
        .aliases(Collections.singletonList(fsObject.getPath()))
        .size(fsObject.getSize())
        .checksums(FileService.makeChecksums(fsObject));
  }

  private List<DRSContentsObject> makeContentsList(FSDir fsDir, SnapshotCacheResult snapshot) {
    List<DRSContentsObject> contentsList = new ArrayList<>();

    for (FSItem fsObject : fsDir.getContents()) {
      contentsList.add(makeDrsContentsObject(fsObject, snapshot));
    }

    return contentsList;
  }

  private DRSContentsObject makeDrsContentsObject(FSItem fsObject, SnapshotCacheResult snapshot) {
    DrsId drsId;
    if (snapshot.globalFileIds) {
      drsId = drsIdService.makeDrsId(fsObject);
    } else {
      drsId = drsIdService.makeDrsId(fsObject, snapshot.id().toString());
    }

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
        contentsObject.contents(makeContentsList(fsDir, snapshot));
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
    if (path.equals("/")) {
      return "";
    }
    String[] pathParts = StringUtils.split(path, '/');
    return pathParts[pathParts.length - 1];
  }

  private SnapshotProject getSnapshotProject(UUID snapshotId) {
    return snapshotProjectsCache.computeIfAbsent(
        snapshotId, snapshotService::retrieveSnapshotProject);
  }

  private SnapshotCacheResult getSnapshot(UUID snapshotId) {
    return snapshotCache.computeIfAbsent(
        snapshotId, id -> new SnapshotCacheResult(snapshotService.retrieve(id)));
  }

  private SnapshotSummaryModel getSnapshotSummary(UUID snapshotId) {
    return snapshotSummariesCache.computeIfAbsent(
        snapshotId, snapshotService::retrieveSnapshotSummary);
  }

  public String registerDrsAliases(List<DrsAliasModel> aliases, AuthenticatedUserRequest userReq) {
    return jobService
        .newJob("Register DRS Aliases", DrsAliasRegisterFlight.class, aliases, userReq)
        .submit();
  }

  @VisibleForTesting
  DRSObject mergeDRSObjects(List<DRSObject> drsObjects) {
    boolean isDirectory =
        drsObjects.stream().map(DRSObject::getAccessMethods).anyMatch(Objects::isNull);
    if (isDirectory
        && drsObjects.stream().map(DRSObject::getAccessMethods).anyMatch(Objects::nonNull)) {
      throw new InvalidDrsObjectException(
          "Drs object would contain a mix of file and directory objects");
    }
    DRSObject drsObject =
        new DRSObject()
            // Extract singleton values
            .id(extractUniqueDrsObjectValue(drsObjects, DRSObject::getId))
            .name(extractUniqueDrsObjectValue(drsObjects, DRSObject::getName))
            .description(extractUniqueDrsObjectValue(drsObjects, DRSObject::getDescription))
            .size(extractUniqueDrsObjectValue(drsObjects, DRSObject::getSize))
            .selfUri(extractUniqueDrsObjectValue(drsObjects, DRSObject::getSelfUri))
            .mimeType(extractUniqueDrsObjectValue(drsObjects, DRSObject::getMimeType))
            .version(extractUniqueDrsObjectValue(drsObjects, DRSObject::getVersion))
            .createdTime(getMinCreatedTime(drsObjects))
            .updatedTime(getMaxUpdatedTime(drsObjects))
            .accessMethods(
                isDirectory
                    ? null
                    : extractDistinctListOfDrsObjectValues(
                        drsObjects,
                        DRSObject::getAccessMethods,
                        Comparator.comparing(DRSAccessMethod::getRegion)
                            .thenComparing(DRSAccessMethod::getType)))
            .checksums(
                extractDistinctListOfDrsObjectValues(
                    drsObjects,
                    DRSObject::getChecksums,
                    Comparator.comparing(DRSChecksum::getType)))
            .aliases(
                extractDistinctListOfDrsObjectValues(
                    drsObjects, DRSObject::getAliases, Comparator.naturalOrder()))
            .contents(
                isDirectory
                    ? extractDistinctListOfDrsObjectValues(
                        drsObjects,
                        DRSObject::getContents,
                        Comparator.comparing(DRSContentsObject::getName))
                    : null);

    // If there are overlapping checksum types with different values, throw an error
    Map<String, List<DRSChecksum>> checksumsByType =
        drsObject.getChecksums().stream().collect(Collectors.groupingBy(DRSChecksum::getType));
    checksumsByType.forEach(
        (k, v) -> {
          if (v.size() > 1) {
            throw new InvalidDrsObjectException(
                "Invalid DRS object. Many checksums for %s exist: %s"
                    .formatted(
                        k,
                        v.stream().map(DRSChecksum::getChecksum).collect(Collectors.joining(","))));
          }
        });
    return drsObject;
  }

  @VisibleForTesting
  static String getMinCreatedTime(List<DRSObject> drsObjects) {
    return drsObjects.stream()
        .map(DRSObject::getCreatedTime)
        .map(Instant::parse)
        .min(Comparator.naturalOrder())
        .map(Instant::toString)
        .orElse("");
  }

  @VisibleForTesting
  static String getMaxUpdatedTime(List<DRSObject> drsObjects) {
    return drsObjects.stream()
        .map(DRSObject::getUpdatedTime)
        .map(Instant::parse)
        .max(Comparator.naturalOrder())
        .map(Instant::toString)
        .orElse("");
  }

  /**
   * Given a list of DRSObjects, extract a singleton value and fail if there are 0, 2 or more values
   */
  @VisibleForTesting
  static <R> R extractUniqueDrsObjectValue(
      List<DRSObject> drsObjects, Function<DRSObject, ? extends R> mapper) {
    List<R> values = new ArrayList<>();
    try {
      values.addAll(drsObjects.stream().map(mapper).distinct().toList());
      return CollectionUtils.extractSingleton(values);
    } catch (IllegalArgumentException e) {
      throw new InvalidDrsObjectException("Found duplicate values: %s".formatted(values), e);
    }
  }

  /** Given a list of DRSObjects, extract a list of distinct values sorted by the comparator. */
  @VisibleForTesting
  static <R> List<R> extractDistinctListOfDrsObjectValues(
      List<DRSObject> drsObjects,
      Function<DRSObject, ? extends Collection<R>> mapper,
      Comparator<R> comparator) {
    return drsObjects.stream()
        .map(mapper)
        .flatMap(Collection::stream)
        .distinct()
        .sorted(comparator)
        .toList();
  }

  record SnapshotCacheResult(
      UUID id,
      String name,
      boolean isSelfHosted,
      boolean globalFileIds,
      BillingProfileModel datasetBillingProfileModel,
      UUID snapshotBillingProfileId,
      CloudPlatform cloudPlatform,
      String googleProjectId,
      String datasetProjectId,
      UUID datasetId,
      String datasetName) {
    public SnapshotCacheResult(Snapshot snapshot) {
      this(
          snapshot.getId(),
          snapshot.getName(),
          snapshot.isSelfHosted(),
          snapshot.hasGlobalFileIds(),
          snapshot.getSourceDataset().getDatasetSummary().getDefaultBillingProfile(),
          snapshot.getProfileId(),
          snapshot.getCloudPlatform(),
          Optional.ofNullable(snapshot.getProjectResource())
              .map(GoogleProjectResource::getGoogleProjectId)
              .orElse(null),
          Optional.ofNullable(snapshot.getSourceDataset().getProjectResource())
              .map(GoogleProjectResource::getGoogleProjectId)
              .orElse(null),
          snapshot.getSourceDataset().getId(),
          snapshot.getSourceDataset().getName());
    }
  }
}
