package bio.terra.service.snapshot;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;

import bio.terra.app.controller.SnapshotsApiController;
import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.model.RASv1Dot1VisaCriterion;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import bio.terra.grammar.Query;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.auth.ras.exception.InvalidAuthorizationMethod;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.exception.SnapshotPreviewException;
import bio.terra.service.snapshot.flight.create.SnapshotCreateFlight;
import bio.terra.service.snapshot.flight.delete.SnapshotDeleteFlight;
import bio.terra.service.snapshot.flight.export.ExportMapKeys;
import bio.terra.service.snapshot.flight.export.SnapshotExportFlight;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import com.google.common.annotations.VisibleForTesting;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SnapshotService {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotService.class);
  private final JobService jobService;
  private final DatasetService datasetService;
  private final FireStoreDependencyDao dependencyDao;
  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotTableDao snapshotTableDao;
  private final MetadataDataAccessUtils metadataDataAccessUtils;
  private final IamService iamService;
  private final EcmService ecmService;

  private final AzureSynapsePdao azureSynapsePdao;

  @Autowired
  public SnapshotService(
      JobService jobService,
      DatasetService datasetService,
      FireStoreDependencyDao dependencyDao,
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      SnapshotDao snapshotDao,
      SnapshotTableDao snapshotTableDao,
      MetadataDataAccessUtils metadataDataAccessUtils,
      IamService iamService,
      EcmService ecmService,
      AzureSynapsePdao azureSynapsePdao) {
    this.jobService = jobService;
    this.datasetService = datasetService;
    this.dependencyDao = dependencyDao;
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotTableDao = snapshotTableDao;
    this.metadataDataAccessUtils = metadataDataAccessUtils;
    this.iamService = iamService;
    this.ecmService = ecmService;
    this.azureSynapsePdao = azureSynapsePdao;
  }

  /**
   * Kick-off snapshot creation Pre-condition: the snapshot request has been syntax checked by the
   * validator
   *
   * @param snapshotRequestModel
   * @returns jobId (flightId) of the job
   */
  public String createSnapshot(
      SnapshotRequestModel snapshotRequestModel, AuthenticatedUserRequest userReq) {
    String description = "Create snapshot " + snapshotRequestModel.getName();
    String sourceDatasetName = snapshotRequestModel.getContents().get(0).getDatasetName();
    Dataset dataset = datasetService.retrieveByName(sourceDatasetName);
    return jobService
        .newJob(description, SnapshotCreateFlight.class, snapshotRequestModel, userReq)
        .addParameter(CommonMapKeys.CREATED_AT, Instant.now().toEpochMilli())
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), dataset.getId())
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.LINK_SNAPSHOT)
        .submit();
  }

  public void undoCreateSnapshot(String snapshotName) throws InterruptedException {
    // Remove any file dependencies created
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotName);
    for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
      Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
      dependencyDao.deleteSnapshotFileDependencies(dataset, snapshot.getId().toString());
    }

    bigQuerySnapshotPdao.deleteSnapshot(snapshot);
  }

  /**
   * Kick-off snapshot deletion
   *
   * @param id snapshot id to delete
   * @returns jobId (flightId) of the job
   */
  public String deleteSnapshot(UUID id, AuthenticatedUserRequest userReq) {
    String description = "Delete snapshot " + id;
    return jobService
        .newJob(description, SnapshotDeleteFlight.class, null, userReq)
        .addParameter(JobMapKeys.SNAPSHOT_ID.getKeyName(), id.toString())
        .addParameter(CommonMapKeys.CREATED_AT, Instant.now().toEpochMilli())
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASNAPSHOT)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.DELETE)
        .submit();
  }

  /**
   * Conditionally require sharing privileges when a caller is updating a passport identifier. Such
   * a modification indirectly affects who can access the underlying data.
   *
   * @param patchRequest updates to merge with an existing snapshot
   * @return IAM actions needed to apply the requested patch
   */
  public Set<IamAction> patchSnapshotIamActions(SnapshotPatchRequestModel patchRequest) {
    Set<IamAction> actions = EnumSet.of(IamAction.UPDATE_SNAPSHOT);
    if (patchRequest.getConsentCode() != null) {
      actions.add(IamAction.UPDATE_PASSPORT_IDENTIFIER);
    }
    return actions;
  }

  public SnapshotSummaryModel patch(UUID id, SnapshotPatchRequestModel patchRequest) {
    boolean patchSucceeded = snapshotDao.patch(id, patchRequest);
    if (!patchSucceeded) {
      throw new RuntimeException("Snapshot was not updated");
    }
    return snapshotDao.retrieveSummaryById(id).toModel();
  }

  public String exportSnapshot(
      UUID id,
      AuthenticatedUserRequest userReq,
      boolean exportGsPaths,
      boolean validatePrimaryKeyUniqueness) {
    String description = "Export snapshot " + id;
    // TODO: add parameters to share job status using a new SAM role to export data
    return jobService
        .newJob(description, SnapshotExportFlight.class, null, userReq)
        .addParameter(JobMapKeys.SNAPSHOT_ID.getKeyName(), id.toString())
        .addParameter(ExportMapKeys.EXPORT_GSPATHS, exportGsPaths)
        .addParameter(ExportMapKeys.EXPORT_VALIDATE_PK_UNIQUENESS, validatePrimaryKeyUniqueness)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASNAPSHOT)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id.toString())
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.EXPORT_SNAPSHOT)
        .submit();
  }

  /**
   * @param userReq authenticated user
   * @return accessible snapshot UUIDs and the IamRoles dictating their accessibility, established
   *     directly by SAM and indirectly by any linked RAS passport
   */
  public Map<UUID, Set<IamRole>> listAuthorizedSnapshots(AuthenticatedUserRequest userReq) {
    Map<UUID, Set<IamRole>> rasAuthorizedSnapshots = new HashMap<>();
    try {
      rasAuthorizedSnapshots = listRasAuthorizedSnapshots(userReq);
    } catch (Exception ex) {
      // In the absence of robust ECM retries / nuanced error handling,
      // do not fail snapshot enumeration because of an ECM error.
      // Ticket for further investment: https://broadworkbench.atlassian.net/browse/DR-2674
      logger.error(
          String.format("Error listing RAS-authorized snapshots for user %s", userReq.getEmail()),
          ex);
    }

    return combineIdsAndRoles(
        iamService.listAuthorizedResources(userReq, IamResourceType.DATASNAPSHOT),
        rasAuthorizedSnapshots);
  }

  /**
   * @param userReq authenticated user
   * @return accessible snapshot UUIDs and the IamRoles dictating their accessibility, established
   *     indirectly by any linked RAS passport
   */
  public Map<UUID, Set<IamRole>> listRasAuthorizedSnapshots(AuthenticatedUserRequest userReq) {
    Map<UUID, Set<IamRole>> idsAndRoles = new HashMap<>();
    try {
      List<RasDbgapPermissions> permissions = ecmService.getRasDbgapPermissions(userReq);
      List<UUID> uuids = snapshotDao.getAccessibleSnapshots(permissions);
      Set<IamRole> roles = Set.of(IamRole.READER);
      uuids.forEach(uuid -> idsAndRoles.put(uuid, roles));
    } catch (ParseException ex) {
      logger.error("Error parsing RAS passport", ex);
    }
    return idsAndRoles;
  }

  /**
   * @param idsAndRoles maps to merge
   * @return a single map of UUIDs to the union of their corresponding IamRoles
   */
  public Map<UUID, Set<IamRole>> combineIdsAndRoles(Map<UUID, Set<IamRole>>... idsAndRoles) {
    return Stream.of(idsAndRoles)
        .flatMap(m -> m.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (roles1, roles2) ->
                    Stream.concat(roles1.stream(), roles2.stream()).collect(Collectors.toSet())));
  }

  /**
   * Enumerate a range of snapshots ordered by created date for consistent offset processing
   *
   * @param offset
   * @param limit
   * @return list of summary models of snapshot
   */
  public EnumerateSnapshotModel enumerateSnapshots(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      List<UUID> datasetIds,
      Map<UUID, Set<IamRole>> idsAndRoles) {
    if (idsAndRoles.isEmpty()) {
      return new EnumerateSnapshotModel().total(0).items(List.of());
    }
    var enumeration =
        snapshotDao.retrieveSnapshots(
            offset, limit, sort, direction, filter, region, datasetIds, idsAndRoles.keySet());
    List<SnapshotSummaryModel> models =
        enumeration.getItems().stream().map(SnapshotSummary::toModel).collect(Collectors.toList());

    Map<String, List<String>> roleMap = new HashMap<>();
    for (SnapshotSummary summary : enumeration.getItems()) {
      var roles =
          idsAndRoles.get(summary.getId()).stream()
              .map(IamRole::toString)
              .collect(Collectors.toList());
      roleMap.put(summary.getId().toString(), roles);
    }
    return new EnumerateSnapshotModel()
        .items(models)
        .total(enumeration.getTotal())
        .filteredTotal(enumeration.getFilteredTotal())
        .roleMap(roleMap);
  }

  /**
   * Return a single snapshot summary given the snapshot id. This is used in the create snapshot
   * flight to build the model response of the asynchronous job.
   *
   * @param id
   * @return summary model of the snapshot
   */
  public SnapshotSummaryModel retrieveSnapshotSummary(UUID id) {
    SnapshotSummary snapshotSummary = snapshotDao.retrieveSummaryById(id);
    return snapshotSummary.toModel();
  }

  /**
   * Convenience wrapper around fetching an existing Snapshot object and converting it to a Model
   * object. Unlike the Snapshot object, the Model object includes a reference to the associated
   * cloud project.
   *
   * <p>Note that this method will only return a snapshot if it is NOT exclusively locked. It is
   * intended for user-facing calls (e.g. from RepositoryApiController), not internal calls that may
   * require an exclusively locked snapshot to be returned (e.g. snapshot deletion).
   *
   * @param id in UUID format
   * @param userRequest Authenticated user object
   * @return a SnapshotModel = API output-friendly representation of the Snapshot
   */
  public SnapshotModel retrieveAvailableSnapshotModel(
      UUID id, AuthenticatedUserRequest userRequest) {
    return retrieveAvailableSnapshotModel(id, getDefaultIncludes(), userRequest);
  }

  /**
   * Convenience wrapper around fetching an existing Snapshot object and converting it to a Model
   * object.
   *
   * <p>Unlike the Snapshot object, the Model object includes a reference to the associated cloud
   * project.
   *
   * <p>Note that this method will only return a snapshot if it is NOT exclusively locked. It is
   * intended for user-facing calls (e.g. from RepositoryApiController), not internal calls that may
   * require an exclusively locked snapshot to be returned (e.g. snapshot deletion).
   *
   * @param id in UUID format
   * @param include a list of what information to include
   * @param userRequest Authenticated user object
   * @return an API output-friendly representation of the Snapshot
   */
  public SnapshotModel retrieveAvailableSnapshotModel(
      UUID id, List<SnapshotRetrieveIncludeModel> include, AuthenticatedUserRequest userRequest) {
    Snapshot snapshot = retrieveAvailable(id);
    return populateSnapshotModelFromSnapshot(snapshot, include, userRequest);
  }

  /**
   * Fetch existing Snapshot object using the id.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public Snapshot retrieve(UUID id) {
    return snapshotDao.retrieveSnapshot(id);
  }

  /**
   * Fetch existing Snapshot object's tables using the id.
   *
   * @param id in UUID format
   * @return a list of snapshot tables
   */
  public List<SnapshotTable> retrieveTables(UUID id) {
    return snapshotDao.retrieveSnapshot(id).getTables();
  }

  /**
   * Fetch existing Snapshot object that is NOT exclusively locked.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public Snapshot retrieveAvailable(UUID id) {
    return snapshotDao.retrieveAvailableSnapshot(id);
  }

  /**
   * Fetch existing Snapshot object that is NOT exclusively locked.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public SnapshotProject retrieveAvailableSnapshotProject(UUID id) {
    return snapshotDao.retrieveAvailableSnapshotProject(id);
  }

  /**
   * Fetch existing Snapshot object using the name.
   *
   * @param name
   * @return a Snapshot object
   */
  public Snapshot retrieveByName(String name) {
    return snapshotDao.retrieveSnapshotByName(name);
  }

  /**
   * Make a Snapshot structure with all of its parts from an incoming snapshot request. Note that
   * the structure does not have UUIDs or created dates filled in. Those are updated by the DAO when
   * it stores the snapshot in the repository metadata.
   *
   * @param snapshotRequestModel
   * @return Snapshot
   */
  public Snapshot makeSnapshotFromSnapshotRequest(SnapshotRequestModel snapshotRequestModel) {
    // Make this early so we can hook up back links to it
    Snapshot snapshot = new Snapshot();
    List<SnapshotRequestContentsModel> requestContentsList = snapshotRequestModel.getContents();
    // TODO: for MVM we only allow one source list
    if (requestContentsList.size() > 1) {
      throw new ValidationException("Only a single snapshot contents entry is currently allowed.");
    }

    SnapshotRequestContentsModel requestContents = requestContentsList.get(0);
    Dataset dataset = datasetService.retrieveByName(requestContents.getDatasetName());
    SnapshotSource snapshotSource = new SnapshotSource().snapshot(snapshot).dataset(dataset);
    switch (snapshotRequestModel.getContents().get(0).getMode()) {
      case BYASSET:
        // TODO: When we implement explicit definition of snapshot tables, we will handle that here.
        // For now, we generate the snapshot tables directly from the asset tables of the one source
        // allowed in a snapshot.
        AssetSpecification assetSpecification = getAssetSpecificationFromRequest(requestContents);
        snapshotSource.assetSpecification(assetSpecification);
        conjureSnapshotTablesFromAsset(
            snapshotSource.getAssetSpecification(), snapshot, snapshotSource);
        break;
      case BYFULLVIEW:
        conjureSnapshotTablesFromDatasetTables(snapshot, snapshotSource);
        break;
      case BYQUERY:
        SnapshotRequestQueryModel queryModel = requestContents.getQuerySpec();
        String assetName = queryModel.getAssetName();
        String snapshotQuery = queryModel.getQuery();
        Query query = Query.parse(snapshotQuery);
        List<String> datasetNames = query.getDatasetNames();
        // TODO this makes the assumption that there is only one dataset
        // (based on the validation flight step that already occurred.)
        // This will change when more than 1 dataset is allowed
        String datasetName = datasetNames.get(0);
        Dataset queryDataset = datasetService.retrieveByName(datasetName);
        AssetSpecification queryAssetSpecification =
            queryDataset
                .getAssetSpecificationByName(assetName)
                .orElseThrow(
                    () ->
                        new AssetNotFoundException(
                            "This dataset does not have an asset specification with name: "
                                + assetName));
        snapshotSource.assetSpecification(queryAssetSpecification);
        // TODO this is wrong? why dont we just pass the assetSpecification?
        conjureSnapshotTablesFromAsset(
            snapshotSource.getAssetSpecification(), snapshot, snapshotSource);
        break;
      case BYROWID:
        SnapshotRequestRowIdModel requestRowIdModel = requestContents.getRowIdSpec();
        conjureSnapshotTablesFromRowIds(requestRowIdModel, snapshot, snapshotSource);
        break;
      default:
        throw new InvalidSnapshotException("Snapshot does not have required mode information");
    }

    return snapshot
        .name(snapshotRequestModel.getName())
        .description(snapshotRequestModel.getDescription())
        .snapshotSources(Collections.singletonList(snapshotSource))
        .profileId(snapshotRequestModel.getProfileId())
        .relationships(createSnapshotRelationships(dataset.getRelationships(), snapshotSource))
        .creationInformation(requestContents)
        .consentCode(snapshotRequestModel.getConsentCode())
        .properties(snapshotRequestModel.getProperties());
  }

  public List<UUID> getSourceDatasetIdsFromSnapshotRequest(
      SnapshotRequestModel snapshotRequestModel) {
    return getSourceDatasetsFromSnapshotRequest(snapshotRequestModel).stream()
        .map(Dataset::getId)
        .collect(Collectors.toList());
  }

  public List<Dataset> getSourceDatasetsFromSnapshotRequest(
      SnapshotRequestModel snapshotRequestModel) {
    return snapshotRequestModel.getContents().stream()
        .map(c -> datasetService.retrieveByName(c.getDatasetName()))
        .collect(Collectors.toList());
  }

  public UUID getFirstSourceDatasetIdFromSnapshotId(
      UUID snapshotId, AuthenticatedUserRequest userRequest) {
    SnapshotModel snapshotModel = retrieveAvailableSnapshotModel(snapshotId, userRequest);
    return snapshotModel.getSource().stream()
        .map(s -> s.getDataset().getId())
        .findFirst()
        .orElseThrow(() -> new DatasetNotFoundException("Source dataset for snapshot not found"));
  }

  /**
   * @param snapshotId snapshot UUID
   * @param userReq authenticated user
   * @return SAM-derived roles held by the user and, if not redundant, any roles derived from the
   *     user's linked RAS passport.
   */
  public List<String> retrieveUserSnapshotRoles(UUID snapshotId, AuthenticatedUserRequest userReq) {
    List<String> roles = new ArrayList<>();
    roles.addAll(iamService.retrieveUserRoles(userReq, IamResourceType.DATASNAPSHOT, snapshotId));
    if (!roles.contains(IamRole.READER.toString())
        && snapshotAccessibleByPassport(snapshotId, userReq).accessible()) {
      roles.add(IamRole.READER.toString());
    }
    return roles;
  }

  /**
   * @param snapshotSummary
   * @param passports RAS passports as JWT tokens
   * @return ValidatePassportResult indicating whether the snapshot's contents are accessible via
   *     one of the supplied RAS passports
   */
  public ValidatePassportResult verifyPassportAuth(
      SnapshotSummaryModel snapshotSummary, List<String> passports) {
    if (passports.isEmpty()) {
      throw new InvalidAuthorizationMethod("No RAS Passports supplied for accessing snapshot");
    }
    if (!SnapshotSummary.passportAuthorizationAvailable(snapshotSummary)) {
      throw new InvalidAuthorizationMethod("Snapshot cannot be accessed by RAS Passports");
    }
    String phsId = snapshotSummary.getPhsId();
    String consentCode = snapshotSummary.getConsentCode();
    var criterion = new RASv1Dot1VisaCriterion().consentCode(consentCode).phsId(phsId);
    ecmService.addRasIssuerAndType(criterion);

    var validatePassportRequest =
        new ValidatePassportRequest().passports(passports).criteria(List.of(criterion));

    return ecmService.validatePassport(validatePassportRequest);
  }

  @VisibleForTesting
  public String passportInvalidForSnapshotErrorMsg(String userEmail) {
    return String.format(
        "Snapshot's passport criteria do not match %s's linked RAS passport", userEmail);
  }

  public record SnapshotAccessibleResult(boolean accessible, List<String> causes) {}

  /**
   * @param snapshotId snapshot UUID
   * @param userReq authenticated user
   * @return a SnapshotAccessibleResult indicating whether the snapshot is accessible to the user
   *     via a linked passport, and if not, any throwable causes of that inaccessibility.
   */
  public SnapshotAccessibleResult snapshotAccessibleByPassport(
      UUID snapshotId, AuthenticatedUserRequest userReq) {
    SnapshotSummaryModel snapshotSummary = snapshotDao.retrieveSummaryById(snapshotId).toModel();
    boolean accessible = false;
    List<String> causes = new ArrayList<>();
    try {
      String passport = ecmService.getRasProviderPassport(userReq);
      if (passport != null) {
        if (verifyPassportAuth(snapshotSummary, List.of(passport)).isValid()) {
          accessible = true;
        } else {
          causes.add(passportInvalidForSnapshotErrorMsg(userReq.getEmail()));
        }
      }
    } catch (Exception ecmEx) {
      logger.warn("Error fetching linked RAS passport", ecmEx);
      causes.add(ecmEx.getMessage());
    }
    return new SnapshotAccessibleResult(accessible, causes);
  }

  /**
   * Throw if the user cannot access the snapshot via SAM permissions or linked RAS passports.
   *
   * @param snapshotId snapshot UUID
   * @param userReq authenticated user
   */
  public void verifySnapshotAccessible(UUID snapshotId, AuthenticatedUserRequest userReq) {
    boolean iamAuthorized = false;
    boolean ecmAuthorized = false;
    List<String> causes = new ArrayList<>();
    try {
      iamService.verifyAuthorization(
          userReq, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
      iamAuthorized = true;
    } catch (Exception iamEx) {
      logger.warn(
          String.format(
              "Snapshot %s inaccessible via SAM for %s, checking for linked RAS passport",
              snapshotId, userReq.getEmail()),
          iamEx);
      causes.add(iamEx.getMessage());
      SnapshotAccessibleResult byPassport = snapshotAccessibleByPassport(snapshotId, userReq);
      ecmAuthorized = byPassport.accessible;
      causes.addAll(byPassport.causes);
    } finally {
      if (!(iamAuthorized || ecmAuthorized)) {
        throw new UnauthorizedException("Error accessing snapshot: see errorDetails", causes);
      }
    }
  }

  public SnapshotPreviewModel retrievePreview(
      AuthenticatedUserRequest userRequest,
      UUID snapshotId,
      String tableName,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter) {
    Snapshot snapshot = retrieve(snapshotId);

    SnapshotTable table =
        snapshot
            .getTableByName(tableName)
            .orElseThrow(
                () ->
                    new SnapshotPreviewException(
                        "No snapshot table exists with the name: " + tableName));

    if (!sort.equalsIgnoreCase(PDAO_ROW_ID_COLUMN)) {
      table
          .getColumnByName(sort)
          .orElseThrow(
              () ->
                  new SnapshotPreviewException(
                      "No snapshot table column exists with the name: " + sort));
    }

    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(
            snapshot.getFirstSnapshotSource().getDataset().getDatasetSummary().getCloudPlatform());

    if (cloudPlatformWrapper.isGcp()) {
      try {
        List<String> columns =
            snapshotTableDao.retrieveColumns(table).stream().map(Column::getName).toList();

        List<Map<String, Object>> values =
            bigQuerySnapshotPdao.getSnapshotTable(
                snapshot, tableName, columns, limit, offset, sort, direction, filter);

        return new SnapshotPreviewModel().result(List.copyOf(values));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SnapshotPreviewException(
            "Error retrieving preview for snapshot " + snapshot.getName(), e);
      }
    } else if (cloudPlatformWrapper.isAzure()) {
      AccessInfoModel accessInfoModel =
          metadataDataAccessUtils.accessInfoFromSnapshot(snapshot, userRequest, tableName);
      String credName = AzureSynapsePdao.getCredentialName(snapshot, userRequest.getEmail());
      String datasourceName = AzureSynapsePdao.getDataSourceName(snapshot, userRequest.getEmail());
      String metadataUrl =
          "%s?%s"
              .formatted(
                  accessInfoModel.getParquet().getUrl(),
                  accessInfoModel.getParquet().getSasToken());

      try {
        azureSynapsePdao.getOrCreateExternalDataSource(metadataUrl, credName, datasourceName);
      } catch (Exception e) {
        throw new RuntimeException("Could not configure external datasource", e);
      }

      List<Map<String, Optional<Object>>> values =
          azureSynapsePdao.getSnapshotTableData(
              userRequest, snapshot, tableName, limit, offset, sort, direction, filter);
      return new SnapshotPreviewModel().result(List.copyOf(values));
    } else {
      throw new SnapshotPreviewException("Cloud not supported");
    }
  }

  private AssetSpecification getAssetSpecificationFromRequest(
      SnapshotRequestContentsModel requestContents) {
    SnapshotRequestAssetModel requestAssetModel = requestContents.getAssetSpec();
    Dataset dataset = datasetService.retrieveByName(requestContents.getDatasetName());

    Optional<AssetSpecification> optAsset =
        dataset.getAssetSpecificationByName(requestAssetModel.getAssetName());
    if (!optAsset.isPresent()) {
      throw new AssetNotFoundException(
          "Asset specification not found: " + requestAssetModel.getAssetName());
    }

    // the map construction will go here. For MVM, we generate the mapping data directly from the
    // asset spec.
    return optAsset.get();
  }

  /**
   * Magic up the snapshot tables and snapshot map from the asset tables and columns. This method
   * sets the table lists into snapshot and snapshotSource.
   *
   * @param asset the one and only asset specification for this snapshot
   * @param snapshot snapshot to point back to and fill in
   * @param snapshotSource snapshotSource to point back to and fill in
   */
  private void conjureSnapshotTablesFromAsset(
      AssetSpecification asset, Snapshot snapshot, SnapshotSource snapshotSource) {

    List<SnapshotTable> tableList = new ArrayList<>();
    List<SnapshotMapTable> mapTableList = new ArrayList<>();

    for (AssetTable assetTable : asset.getAssetTables()) {
      // Create early so we can hook up back pointers.
      SnapshotTable table = new SnapshotTable();

      // Build the column lists in parallel, so we can easily connect the
      // map column to the snapshot column.
      List<Column> columnList = new ArrayList<>();
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();

      for (AssetColumn assetColumn : assetTable.getColumns()) {
        Column column = new Column(assetColumn.getDatasetColumn());
        columnList.add(column);

        mapColumnList.add(
            new SnapshotMapColumn().fromColumn(assetColumn.getDatasetColumn()).toColumn(column));
      }

      table
          .name(assetTable.getTable().getName())
          .primaryKey(assetTable.getTable().getPrimaryKey())
          .columns(columnList);
      tableList.add(table);
      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(assetTable.getTable())
              .toTable(table)
              .snapshotMapColumns(mapColumnList));
    }

    snapshotSource.snapshotMapTables(mapTableList);
    snapshot.snapshotTables(tableList);
  }

  /**
   * Map from a list of source relationships (from a dataset or asset) into snapshot relationships.
   *
   * @param sourceRelationships relationships from a dataset or asset
   * @param snapshotSource source with mapping between dataset tables and columns -> snapshot tables
   *     and columns
   * @return a list of relationships tied to the snapshot tables
   */
  public List<Relationship> createSnapshotRelationships(
      List<Relationship> sourceRelationships, SnapshotSource snapshotSource) {
    // We'll copy the asset relationships into the snapshot.
    List<Relationship> snapshotRelationships = new ArrayList<>();

    // Create lookups from dataset table and column ids -> snapshot tables and columns, respectively
    Map<UUID, Table> tableLookup = new HashMap<>();
    Map<UUID, Column> columnLookup = new HashMap<>();

    for (SnapshotMapTable mapTable : snapshotSource.getSnapshotMapTables()) {
      tableLookup.put(mapTable.getFromTable().getId(), mapTable.getToTable());

      for (SnapshotMapColumn mapColumn : mapTable.getSnapshotMapColumns()) {
        columnLookup.put(mapColumn.getFromColumn().getId(), mapColumn.getToColumn());
      }
    }

    for (Relationship sourceRelationship : sourceRelationships) {
      UUID fromTableId = sourceRelationship.getFromTable().getId();
      UUID fromColumnId = sourceRelationship.getFromColumn().getId();
      UUID toTableId = sourceRelationship.getToTable().getId();
      UUID toColumnId = sourceRelationship.getToColumn().getId();

      if (tableLookup.containsKey(fromTableId)
          && tableLookup.containsKey(toTableId)
          && columnLookup.containsKey(fromColumnId)
          && columnLookup.containsKey(toColumnId)) {
        Table fromTable = tableLookup.get(fromTableId);
        Column fromColumn = columnLookup.get(fromColumnId);
        Table toTable = tableLookup.get(toTableId);
        Column toColumn = columnLookup.get(toColumnId);
        snapshotRelationships.add(
            new Relationship()
                .name(sourceRelationship.getName())
                .fromTable(fromTable)
                .fromColumn(fromColumn)
                .toTable(toTable)
                .toColumn(toColumn));
      }
    }
    return snapshotRelationships;
  }

  private void conjureSnapshotTablesFromRowIds(
      SnapshotRequestRowIdModel requestRowIdModel,
      Snapshot snapshot,
      SnapshotSource snapshotSource) {
    // TODO this will need to be changed when we have more than one dataset per snapshot (>1
    // contentsModel)
    List<SnapshotTable> tableList = new ArrayList<>();
    snapshot.snapshotTables(tableList);
    List<SnapshotMapTable> mapTableList = new ArrayList<>();
    snapshotSource.snapshotMapTables(mapTableList);
    Dataset dataset = snapshotSource.getDataset();

    // create a lookup from tableName -> table spec from the request
    Map<String, SnapshotRequestRowIdTableModel> requestTableLookup =
        requestRowIdModel.getTables().stream()
            .collect(
                Collectors.toMap(
                    SnapshotRequestRowIdTableModel::getTableName, Function.identity()));

    // for each dataset table specified in the request, create a table in the snapshot with the same
    // name
    for (DatasetTable datasetTable : dataset.getTables()) {
      if (!requestTableLookup.containsKey(datasetTable.getName())) {
        continue; // only capture the dataset tables in the request model
      }
      List<Column> columnList = new ArrayList<>();
      SnapshotTable snapshotTable =
          new SnapshotTable()
              .name(datasetTable.getName())
              .primaryKey(datasetTable.getPrimaryKey())
              .columns(columnList);
      tableList.add(snapshotTable);
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();
      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(datasetTable)
              .toTable(snapshotTable)
              .snapshotMapColumns(mapColumnList));

      // for each dataset column specified in the request, create a column in the snapshot w the
      // same name & array
      Set<String> requestColumns =
          new HashSet<>(requestTableLookup.get(datasetTable.getName()).getColumns());
      datasetTable.getColumns().stream()
          .filter(c -> requestColumns.contains(c.getName()))
          .forEach(
              datasetColumn -> {
                Column snapshotColumn = Column.toSnapshotColumn(datasetColumn);
                SnapshotMapColumn snapshotMapColumn =
                    new SnapshotMapColumn().fromColumn(datasetColumn).toColumn(snapshotColumn);
                columnList.add(snapshotColumn);
                mapColumnList.add(snapshotMapColumn);
              });
    }
  }

  private void conjureSnapshotTablesFromDatasetTables(
      Snapshot snapshot, SnapshotSource snapshotSource) {
    // TODO this will need to be changed when we have more than one dataset per snapshot (>1
    // contentsModel)
    // for each dataset table specified in the request, create a table in the snapshot with the same
    // name
    Dataset dataset = snapshotSource.getDataset();
    List<SnapshotTable> tableList = new ArrayList<>();
    List<SnapshotMapTable> mapTableList = new ArrayList<>();

    for (DatasetTable datasetTable : dataset.getTables()) {
      List<Column> columnList = new ArrayList<>();
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();

      // for each dataset column specified in the request, create a column in the snapshot w the
      // same name & array
      for (Column datasetColumn : datasetTable.getColumns()) {
        Column snapshotColumn = Column.toSnapshotColumn(datasetColumn);
        SnapshotMapColumn snapshotMapColumn =
            new SnapshotMapColumn().fromColumn(datasetColumn).toColumn(snapshotColumn);
        columnList.add(snapshotColumn);
        mapColumnList.add(snapshotMapColumn);
      }

      // create snapshot tables & mapping with the proper dataset name and columns
      SnapshotTable snapshotTable =
          new SnapshotTable()
              .name(datasetTable.getName())
              .primaryKey(datasetTable.getPrimaryKey())
              .columns(columnList);
      tableList.add(snapshotTable);

      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(datasetTable)
              .toTable(snapshotTable)
              .snapshotMapColumns(mapColumnList));
    }
    // set the snapshot tables and mapping
    snapshot.snapshotTables(tableList);
    snapshotSource.snapshotMapTables(mapTableList);
  }

  private SnapshotModel populateSnapshotModelFromSnapshot(
      Snapshot snapshot,
      List<SnapshotRetrieveIncludeModel> include,
      AuthenticatedUserRequest userRequest) {
    SnapshotModel snapshotModel =
        new SnapshotModel()
            .id(snapshot.getId())
            .name(snapshot.getName())
            .description(snapshot.getDescription())
            .createdDate(snapshot.getCreatedDate().toString())
            .consentCode(snapshot.getConsentCode());

    // In case NONE is specified, this should supersede any other value being passed in
    if (include.contains(SnapshotRetrieveIncludeModel.NONE)) {
      return snapshotModel;
    }

    if (include.contains(SnapshotRetrieveIncludeModel.SOURCES)) {
      snapshotModel.source(
          snapshot.getSnapshotSources().stream()
              .map(this::makeSourceModelFromSource)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRetrieveIncludeModel.TABLES)) {
      snapshotModel.tables(
          snapshot.getTables().stream()
              .map(this::makeTableModelFromTable)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRetrieveIncludeModel.RELATIONSHIPS)) {
      snapshotModel.relationships(
          snapshot.getRelationships().stream()
              .map(this::makeRelationshipModelFromRelationship)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRetrieveIncludeModel.PROFILE)) {
      snapshotModel.profileId(snapshot.getProfileId());
    }
    if (include.contains(SnapshotRetrieveIncludeModel.DATA_PROJECT)) {
      if (snapshot.getProjectResource() != null) {
        snapshotModel.dataProject(snapshot.getProjectResource().getGoogleProjectId());
      }
    }
    if (include.contains(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION)) {
      snapshotModel.accessInformation(
          metadataDataAccessUtils.accessInfoFromSnapshot(snapshot, userRequest));
    }
    if (include.contains(SnapshotRetrieveIncludeModel.CREATION_INFORMATION)) {
      snapshotModel.creationInformation(snapshot.getCreationInformation());
    }
    if (include.contains(SnapshotRetrieveIncludeModel.PROPERTIES)) {
      snapshotModel.properties(snapshot.getProperties());
    }

    return snapshotModel;
  }

  private RelationshipModel makeRelationshipModelFromRelationship(Relationship relationship) {
    RelationshipTermModel fromModel =
        new RelationshipTermModel()
            .table(relationship.getFromTable().getName())
            .column(relationship.getFromColumn().getName());
    RelationshipTermModel toModel =
        new RelationshipTermModel()
            .table(relationship.getToTable().getName())
            .column(relationship.getToColumn().getName());
    return new RelationshipModel().name(relationship.getName()).from(fromModel).to(toModel);
  }

  private SnapshotSourceModel makeSourceModelFromSource(SnapshotSource source) {
    // TODO: when source summary methods are available, use those. Here I roll my own
    Dataset dataset = source.getDataset();
    DatasetSummaryModel summaryModel =
        new DatasetSummaryModel()
            .id(dataset.getId())
            .name(dataset.getName())
            .description(dataset.getDescription())
            .defaultProfileId(dataset.getDefaultProfileId())
            .createdDate(dataset.getCreatedDate().toString())
            .storage(
                dataset.getDatasetSummary().getStorage().stream()
                    .map(StorageResource::toModel)
                    .collect(Collectors.toList()))
            .secureMonitoringEnabled(dataset.isSecureMonitoringEnabled())
            .phsId(dataset.getPhsId())
            .selfHosted(dataset.isSelfHosted());

    SnapshotSourceModel sourceModel =
        new SnapshotSourceModel().dataset(summaryModel).datasetProperties(dataset.getProperties());

    AssetSpecification assetSpec = source.getAssetSpecification();
    if (assetSpec != null) {
      sourceModel.asset(assetSpec.getName());
    }

    return sourceModel;
  }

  // TODO: share these methods with dataset table in some common place
  private TableModel makeTableModelFromTable(Table table) {
    Long rowCount = table.getRowCount();
    return new TableModel()
        .name(table.getName())
        .rowCount(rowCount != null ? rowCount.intValue() : null)
        .primaryKey(
            table.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()))
        .columns(
            table.getColumns().stream()
                .map(this::makeColumnModelFromColumn)
                .collect(Collectors.toList()));
  }

  private ColumnModel makeColumnModelFromColumn(Column column) {
    return new ColumnModel()
        .name(column.getName())
        .datatype(column.getType())
        .arrayOf(column.isArrayOf())
        .required(column.isRequired());
  }

  private static List<SnapshotRetrieveIncludeModel> getDefaultIncludes() {
    return Arrays.stream(
            StringUtils.split(SnapshotsApiController.RETRIEVE_INCLUDE_DEFAULT_VALUE, ','))
        .map(SnapshotRetrieveIncludeModel::fromValue)
        .collect(Collectors.toList());
  }
}
