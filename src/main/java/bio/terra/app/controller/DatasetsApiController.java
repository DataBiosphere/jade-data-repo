package bio.terra.app.controller;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.app.utils.PolicyUtils;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidationUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.DatasetsApi;
import bio.terra.model.AssetModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.ColumnStatisticsModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.UpdateStrategyEnum;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.QueryColumnStatisticsRequestModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotBuilderAccessRequest;
import bio.terra.model.SnapshotBuilderCountRequest;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TagCountResultModel;
import bio.terra.model.TagUpdateRequestModel;
import bio.terra.model.TransactionCloseModel;
import bio.terra.model.TransactionCreateModel;
import bio.terra.model.TransactionModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.DataDeletionRequestValidator;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetRequestValidator;
import bio.terra.service.dataset.DatasetSchemaUpdateValidator;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import io.swagger.annotations.Api;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Api(tags = {"datasets"})
public class DatasetsApiController implements DatasetsApi {
  public static final String RETRIEVE_INCLUDE_DEFAULT_VALUE = "SCHEMA,PROFILE,DATA_PROJECT,STORAGE";

  private final HttpServletRequest request;
  private final JobService jobService;
  private final DatasetRequestValidator datasetRequestValidator;
  private final DatasetService datasetService;
  private final IamService iamService;
  private final FileService fileService;
  private final SnapshotBuilderService snapshotBuilderService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;
  private final IngestRequestValidator ingestRequestValidator;
  private final DataDeletionRequestValidator dataDeletionRequestValidator;
  private final DatasetSchemaUpdateValidator datasetSchemaUpdateValidator;

  @Autowired
  public DatasetsApiController(
      HttpServletRequest request,
      JobService jobService,
      DatasetRequestValidator datasetRequestValidator,
      DatasetService datasetService,
      IamService iamService,
      FileService fileService,
      SnapshotBuilderService snapshotBuilderService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator,
      IngestRequestValidator ingestRequestValidator,
      DataDeletionRequestValidator dataDeletionRequestValidator,
      DatasetSchemaUpdateValidator datasetSchemaUpdateValidator) {
    this.request = request;
    this.jobService = jobService;
    this.datasetRequestValidator = datasetRequestValidator;
    this.datasetService = datasetService;
    this.iamService = iamService;
    this.fileService = fileService;
    this.snapshotBuilderService = snapshotBuilderService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
    this.ingestRequestValidator = ingestRequestValidator;
    this.dataDeletionRequestValidator = dataDeletionRequestValidator;
    this.datasetSchemaUpdateValidator = datasetSchemaUpdateValidator;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(ingestRequestValidator);
    binder.addValidators(datasetRequestValidator);
    binder.addValidators(assetModelValidator);
    binder.addValidators(dataDeletionRequestValidator);
    binder.addValidators(datasetSchemaUpdateValidator);
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  // -- dataset --
  @Override
  public ResponseEntity<JobModel> createDataset(
      @Valid @RequestBody DatasetRequestModel datasetRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = datasetService.createDataset(datasetRequest, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<DatasetModel> retrieveDataset(
      UUID id,
      @RequestParam(
              value = "include",
              required = false,
              defaultValue = RETRIEVE_INCLUDE_DEFAULT_VALUE)
          List<DatasetRequestAccessIncludeModel> include) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifyDatasetAuthorizations(
        userRequest, id.toString(), DatasetService.getRetrieveDatasetRequiredActions(include));

    return ResponseEntity.ok(datasetService.retrieveDatasetModel(id, userRequest, include));
  }

  @Override
  public ResponseEntity<DatasetSummaryModel> retrieveDatasetSummary(UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifyDatasetAuthorization(userRequest, id.toString(), null);
    return ResponseEntity.ok(datasetService.retrieveDatasetSummary(id));
  }

  @Override
  public ResponseEntity<DatasetModel> updateDatasetSnapshotBuilderSettings(
      @PathVariable("id") UUID id, @Valid @RequestBody SnapshotBuilderSettings settings) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest,
        IamResourceType.DATASET,
        id.toString(),
        IamAction.UPDATE_SNAPSHOT_BUILDER_SETTINGS);
    datasetService.updateDatasetSnapshotBuilderSettings(id, settings);
    return ResponseEntity.ok(
        datasetService.retrieveDatasetModel(
            id, userRequest, List.of(DatasetRequestAccessIncludeModel.SNAPSHOT_BUILDER_SETTINGS)));
  }

  @Override
  public ResponseEntity<SnapshotBuilderGetConceptsResponse> getConcepts(
      UUID id, Integer conceptId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest,
        IamResourceType.DATASET,
        id.toString(),
        IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
    return ResponseEntity.ok(snapshotBuilderService.getConceptChildren(id, conceptId));
  }

  @Override
  public ResponseEntity<SnapshotBuilderCountResponse> getSnapshotBuilderCount(
      UUID id, SnapshotBuilderCountRequest body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifyDatasetAuthorization(
        userRequest, id.toString(), IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
    return ResponseEntity.ok(snapshotBuilderService.getCountResponse(id, body.getCohorts()));
  }

  @Override
  public ResponseEntity<DatasetDataModel> queryDatasetDataById(
      UUID id, String table, QueryDataRequestModel queryDataRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.READ_DATA);
    // TODO: Remove after https://broadworkbench.atlassian.net/browse/DR-2588 is fixed
    SqlSortDirection sortDirection =
        Objects.requireNonNullElse(queryDataRequest.getDirection(), SqlSortDirection.ASC);
    DatasetDataModel previewModel =
        datasetService.retrieveData(
            userReq,
            id,
            table,
            queryDataRequest.getLimit(),
            queryDataRequest.getOffset(),
            queryDataRequest.getSort(),
            sortDirection,
            queryDataRequest.getFilter());
    return ResponseEntity.ok(previewModel);
  }

  @Override
  public ResponseEntity<JobModel> lockDataset(UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest, IamResourceType.DATASET, id.toString(), IamAction.LOCK_RESOURCE);
    String jobId = datasetService.manualExclusiveLock(userRequest, id);
    return jobToResponse(jobService.retrieveJob(jobId, userRequest));
  }

  @Override
  public ResponseEntity<JobModel> unlockDataset(UUID id, String lockName) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest, IamResourceType.DATASET, id.toString(), IamAction.UNLOCK_RESOURCE);
    String jobId = datasetService.manualExclusiveUnlock(userRequest, id, lockName);
    return jobToResponse(jobService.retrieveJob(jobId, userRequest));
  }

  @Override
  public ResponseEntity<DatasetDataModel> lookupDatasetDataById(
      UUID id,
      String table,
      Integer offset,
      Integer limit,
      String sort,
      SqlSortDirection direction,
      String filter) {
    var request =
        new QueryDataRequestModel()
            .offset(offset)
            .limit(limit)
            .sort(sort)
            .direction(direction)
            .filter(filter);
    return queryDatasetDataById(id, table, request);
  }

  @Override
  public ResponseEntity<ColumnStatisticsModel> lookupDatasetColumnStatisticsById(
      UUID id, String table, String column, String filter) {
    return queryDatasetColumnStatisticsById(
        id, table, column, new QueryColumnStatisticsRequestModel().filter(filter));
  }

  @Override
  public ResponseEntity<ColumnStatisticsModel> queryDatasetColumnStatisticsById(
      UUID id, String table, String column, QueryColumnStatisticsRequestModel requestModel) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.READ_DATA);
    ColumnStatisticsModel columnStatisticsModel =
        datasetService.retrieveColumnStatistics(
            userReq, id, table, column, requestModel.getFilter());
    return ResponseEntity.ok(columnStatisticsModel);
  }

  @Override
  public ResponseEntity<JobModel> deleteDataset(@PathVariable("id") UUID id) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.DELETE);
    String jobId = datasetService.delete(id.toString(), userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<DatasetSummaryModel> patchDataset(
      UUID id, DatasetPatchRequestModel patchRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    Set<IamAction> actions = datasetService.patchDatasetIamActions(patchRequest);
    verifyDatasetAuthorizations(userReq, id.toString(), actions);
    return ResponseEntity.ok(datasetService.patch(id, patchRequest, userReq));
  }

  @Override
  public ResponseEntity<EnumerateDatasetModel> enumerateDatasets(
      Integer offset,
      Integer limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      List<String> tags) {
    ControllerUtils.validateEnumerateParams(offset, limit);
    var idsAndRoles =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASET);
    var edm =
        datasetService.enumerate(offset, limit, sort, direction, filter, region, idsAndRoles, tags);
    return ResponseEntity.ok(edm);
  }

  @Override
  public ResponseEntity<JobModel> ingestDataset(
      @PathVariable("id") UUID id, @Valid @RequestBody IngestRequestModel ingest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    // Set default strategy to append
    if (ingest.getUpdateStrategy() == null) {
      ingest.updateStrategy(UpdateStrategyEnum.APPEND);
    }
    validateIngestParams(ingest, id);
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = datasetService.ingestDataset(id.toString(), ingest, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> addDatasetAssetSpecifications(
      @PathVariable("id") UUID id, @Valid @RequestBody AssetModel asset) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.MANAGE_SCHEMA);
    String jobId = datasetService.addDatasetAssetSpecifications(id.toString(), asset, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> removeDatasetAssetSpecifications(
      @PathVariable("id") UUID id, @PathVariable("assetid") String assetId) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.MANAGE_SCHEMA);
    String jobId = datasetService.removeDatasetAssetSpecifications(id.toString(), assetId, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> applyDatasetDataDeletion(
      UUID id, @RequestBody @Valid DataDeletionRequest dataDeletionRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.SOFT_DELETE);
    String jobId = datasetService.deleteTabularData(id.toString(), dataDeletionRequest, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  // -- dataset-file --
  @Override
  public ResponseEntity<JobModel> deleteFile(
      @PathVariable("id") UUID id, @PathVariable("fileid") String fileid) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.SOFT_DELETE);
    String jobId = fileService.deleteFile(id.toString(), fileid, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<List<FileModel>> listFiles(UUID id, Integer offset, Integer limit) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifyDatasetAuthorization(userRequest, id.toString(), IamAction.READ_DATASET);
    List<FileModel> results = fileService.listDatasetFiles(id.toString(), offset, limit);
    return ResponseEntity.ok(results);
  }

  @Override
  public ResponseEntity<JobModel> ingestFile(
      @PathVariable("id") UUID id, @Valid @RequestBody FileLoadModel ingestFile) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = fileService.ingestFile(id.toString(), ingestFile, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> bulkFileLoad(
      @PathVariable("id") UUID id, @Valid @RequestBody BulkLoadRequestModel bulkFileLoad) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = fileService.ingestBulkFile(id.toString(), bulkFileLoad, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> bulkFileLoadArray(
      @PathVariable("id") UUID id,
      @Valid @RequestBody BulkLoadArrayRequestModel bulkFileLoadArray) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = fileService.ingestBulkFileArray(id.toString(), bulkFileLoadArray, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<BulkLoadHistoryModelList> getLoadHistoryForLoadTag(
      @PathVariable("id") UUID id,
      @PathVariable("loadTag") String loadTag,
      @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
      @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
    verifyDatasetAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.READ_DATASET);
    List<BulkLoadHistoryModel> history = datasetService.getLoadHistory(id, loadTag, offset, limit);
    return ResponseEntity.ok(new BulkLoadHistoryModelList().total(history.size()).items(history));
  }

  @Override
  public ResponseEntity<FileModel> lookupFileById(
      @PathVariable("id") UUID id,
      @PathVariable("fileid") String fileid,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {
    verifyDatasetAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.READ_DATA);
    FileModel fileModel = fileService.lookupFile(id.toString(), fileid, depth);
    return ResponseEntity.ok(fileModel);
  }

  @Override
  public ResponseEntity<FileModel> lookupFileByPath(
      @PathVariable("id") UUID id,
      @RequestParam(value = "path") String path,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    verifyDatasetAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.READ_DATA);
    if (!ValidationUtils.isValidPath(path)) {
      throw new ValidationException("InvalidPath");
    }
    FileModel fileModel = fileService.lookupPath(id.toString(), path, depth);
    return ResponseEntity.ok(fileModel);
  }

  // --dataset policies --
  @Override
  public ResponseEntity<PolicyResponse> addDatasetPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @Valid @RequestBody PolicyMemberRequest policyMember) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    PolicyModel policy =
        iamService.addPolicyMember(
            userReq, IamResourceType.DATASET, id, policyName, policyMember.getEmail());
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<PolicyResponse> retrieveDatasetPolicies(@PathVariable("id") UUID id) {
    List<SamPolicyModel> policies =
        iamService.retrievePolicies(getAuthenticatedInfo(), IamResourceType.DATASET, id);
    PolicyResponse response =
        new PolicyResponse().policies(PolicyUtils.samToTdrPolicyModels(policies));
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<PolicyResponse> deleteDatasetPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @PathVariable("memberEmail") String memberEmail) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    // member email can't be null since it is part of the URL
    if (!ValidationUtils.isValidEmail(memberEmail)) {
      throw new ValidationException("InvalidMemberEmail");
    }
    PolicyModel policy =
        iamService.deletePolicyMember(
            userReq, IamResourceType.DATASET, id, policyName, memberEmail);
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<List<String>> retrieveUserDatasetRoles(UUID id) {
    List<String> roles =
        iamService.retrieveUserRoles(getAuthenticatedInfo(), IamResourceType.DATASET, id);
    return ResponseEntity.ok(roles);
  }

  @Override
  public ResponseEntity<JobModel> updateSchema(UUID id, DatasetSchemaUpdateModel body) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.MANAGE_SCHEMA);
    String jobId = datasetService.updateDatasetSchema(id, body, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> openTransaction(UUID id, TransactionCreateModel body) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = datasetService.openTransaction(id, body, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> closeTransaction(
      UUID id, UUID transactionId, TransactionCloseModel body) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.INGEST_DATA);
    String jobId = datasetService.closeTransaction(id, transactionId, userReq, body.getMode());
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<List<TransactionModel>> enumerateTransactions(
      UUID id, Integer offset, Integer limit) {
    verifyDatasetAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.INGEST_DATA);
    return ResponseEntity.ok(datasetService.enumerateTransactions(id, offset, limit));
  }

  @Override
  public ResponseEntity<TransactionModel> retrieveTransaction(UUID id, UUID transactionId) {
    verifyDatasetAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.INGEST_DATA);
    return ResponseEntity.ok(datasetService.retrieveTransaction(id, transactionId));
  }

  @Override
  public ResponseEntity<DatasetSummaryModel> updateDatasetTags(
      UUID id, TagUpdateRequestModel tagUpdateRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifyDatasetAuthorization(userReq, id.toString(), IamAction.MANAGE_SCHEMA);
    return ResponseEntity.ok(datasetService.updateTags(id, tagUpdateRequest));
  }

  @Override
  public ResponseEntity<TagCountResultModel> getDatasetTags(String filter, Integer limit) {
    var idsAndRoles =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASET);
    return ResponseEntity.ok(datasetService.getTags(idsAndRoles, filter, limit));
  }

  @Override
  public ResponseEntity<SnapshotBuilderAccessRequest> createSnapshotRequest(
      UUID id, SnapshotBuilderAccessRequest snapshotAccessRequest) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest,
        IamResourceType.DATASET,
        id.toString(),
        IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
    return ResponseEntity.ok(
        snapshotBuilderService.createSnapshotRequest(id, snapshotAccessRequest));
  }

  private void validateIngestParams(IngestRequestModel ingestRequestModel, UUID datasetId) {
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper platform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    if (platform.isAzure()) {
      validateAzureIngestParams(ingestRequestModel);
    }
  }

  private void validateAzureIngestParams(IngestRequestModel ingestRequest) {
    List<String> errors = new ArrayList<>();
    if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.CSV) {
      // validate CSV parameters
      if (ingestRequest.getCsvSkipLeadingRows() == null) {
        errors.add("For CSV ingests, 'csvSkipLeadingRows' must be defined.");
      } else if (ingestRequest.getCsvSkipLeadingRows() < 0) {
        errors.add(
            String.format(
                "'csvSkipLeadingRows' must be a positive integer, was '%d.",
                ingestRequest.getCsvSkipLeadingRows()));
      }

      if (ingestRequest.getCsvFieldDelimiter() == null) {
        errors.add("For CSV ingests, 'csvFieldDelimiter' must be defined.");
      } else if (ingestRequest.getCsvFieldDelimiter().length() != 1) {
        errors.add(
            String.format(
                "'csvFieldDelimiter' must be a single character, was '%s'.",
                ingestRequest.getCsvFieldDelimiter()));
      }

      if (ingestRequest.getCsvQuote() == null) {
        errors.add("For CSV ingests, 'csvQuote' must be defined.");
      } else if (ingestRequest.getCsvQuote().length() != 1) {
        errors.add(
            String.format(
                "'csvQuote' must be a single character, was '%s'.", ingestRequest.getCsvQuote()));
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidJobParameterException("Invalid ingest parameters detected", errors);
    }
  }

  private void verifyDatasetAuthorization(
      AuthenticatedUserRequest userReq, String resourceId, IamAction action) {
    IamResourceType resourceType = IamResourceType.DATASET;
    // Check if dataset exists
    datasetService.retrieveDatasetSummary(UUID.fromString(resourceId));
    // Verify dataset permissions
    if (action != null) {
      iamService.verifyAuthorization(userReq, resourceType, resourceId, action);
    } else {
      iamService.verifyAuthorization(userReq, resourceType, resourceId);
    }
  }

  private void verifyDatasetAuthorizations(
      AuthenticatedUserRequest userReq, String resourceId, Collection<IamAction> actions) {
    // Check if dataset exists
    datasetService.retrieveDatasetSummary(UUID.fromString(resourceId));
    // Verify dataset permissions
    iamService.verifyAuthorizations(userReq, IamResourceType.DATASET, resourceId, actions);
  }
}
