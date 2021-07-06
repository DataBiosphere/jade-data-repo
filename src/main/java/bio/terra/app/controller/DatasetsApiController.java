package bio.terra.app.controller;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.ValidationUtils;
import bio.terra.controller.DatasetsApi;
import bio.terra.model.AssetModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.DatasetRequestValidator;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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

  private Logger logger = LoggerFactory.getLogger(DatasetsApiController.class);

  public static final String RETRIEVE_INCLUDE_DEFAULT_VALUE = "SCHEMA,PROFILE,DATA_PROJECT,STORAGE";

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final JobService jobService;
  private final DatasetRequestValidator datasetRequestValidator;
  private final DatasetService datasetService;
  private final IamService iamService;
  private final FileService fileService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;
  private final IngestRequestValidator ingestRequestValidator;

  @Autowired
  public DatasetsApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      JobService jobService,
      DatasetRequestValidator datasetRequestValidator,
      DatasetService datasetService,
      IamService iamService,
      FileService fileService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator,
      IngestRequestValidator ingestRequestValidator) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.jobService = jobService;
    this.datasetRequestValidator = datasetRequestValidator;
    this.datasetService = datasetService;
    this.iamService = iamService;
    this.fileService = fileService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
    this.ingestRequestValidator = ingestRequestValidator;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(ingestRequestValidator);
    binder.addValidators(datasetRequestValidator);
    binder.addValidators(assetModelValidator);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
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
      @PathVariable("id") UUID id,
      @Valid
          @RequestParam(
              value = "include",
              required = false,
              defaultValue = RETRIEVE_INCLUDE_DEFAULT_VALUE)
          List<DatasetRequestAccessIncludeModel> include) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(), IamResourceType.DATASET, id.toString(), IamAction.READ_DATASET);
    return new ResponseEntity<>(
        datasetService.retrieveAvailableDatasetModel(id, include), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<JobModel> deleteDataset(@PathVariable("id") UUID id) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.DELETE);
    String jobId = datasetService.delete(id.toString(), userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<EnumerateDatasetModel> enumerateDatasets(
      @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
      @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
      @Valid @RequestParam(value = "sort", required = false, defaultValue = "created_date")
          EnumerateSortByParam sort,
      @Valid @RequestParam(value = "direction", required = false, defaultValue = "asc")
          SqlSortDirection direction,
      @Valid @RequestParam(value = "filter", required = false) String filter,
      @Valid @RequestParam(value = "region", required = false) String region) {
    ControllerUtils.validateEnumerateParams(offset, limit);
    List<UUID> resources =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASET);
    EnumerateDatasetModel esm =
        datasetService.enumerate(offset, limit, sort, direction, filter, region, resources);
    return new ResponseEntity<>(esm, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<JobModel> ingestDataset(
      @PathVariable("id") UUID id, @Valid @RequestBody IngestRequestModel ingest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.INGEST_DATA);
    String jobId = datasetService.ingestDataset(id.toString(), ingest, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> addDatasetAssetSpecifications(
      @PathVariable("id") UUID id, @Valid @RequestBody AssetModel asset) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.MANAGE_SCHEMA);
    String jobId = datasetService.addDatasetAssetSpecifications(id.toString(), asset, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> removeDatasetAssetSpecifications(
      @PathVariable("id") UUID id, @PathVariable("assetId") String assetId) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.MANAGE_SCHEMA);
    String jobId = datasetService.removeDatasetAssetSpecifications(id.toString(), assetId, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> applyDatasetDataDeletion(
      UUID id, @RequestBody @Valid DataDeletionRequest dataDeletionRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = datasetService.deleteTabularData(id.toString(), dataDeletionRequest, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  // -- dataset-file --
  @Override
  public ResponseEntity<JobModel> deleteFile(
      @PathVariable("id") UUID id, @PathVariable("fileid") String fileid) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.SOFT_DELETE);
    String jobId = fileService.deleteFile(id.toString(), fileid, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> ingestFile(
      @PathVariable("id") UUID id, @Valid @RequestBody FileLoadModel ingestFile) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASET, id.toString(), IamAction.INGEST_DATA);
    String jobId = fileService.ingestFile(id.toString(), ingestFile, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> bulkFileLoad(
      @PathVariable("id") UUID id, @Valid @RequestBody BulkLoadRequestModel bulkFileLoad) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = fileService.ingestBulkFile(id.toString(), bulkFileLoad, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<JobModel> bulkFileLoadArray(
      @PathVariable("id") UUID id,
      @Valid @RequestBody BulkLoadArrayRequestModel bulkFileLoadArray) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = fileService.ingestBulkFileArray(id.toString(), bulkFileLoadArray, userReq);
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<FileModel> lookupFileById(
      @PathVariable("id") UUID id,
      @PathVariable("fileid") String fileid,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(), IamResourceType.DATASET, id.toString(), IamAction.READ_DATA);
    FileModel fileModel = fileService.lookupFile(id.toString(), fileid, depth);
    return new ResponseEntity<>(fileModel, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<FileModel> lookupFileByPath(
      @PathVariable("id") UUID id,
      @RequestParam(value = "path", required = true) String path,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    iamService.verifyAuthorization(
        getAuthenticatedInfo(), IamResourceType.DATASET, id.toString(), IamAction.READ_DATA);
    if (!ValidationUtils.isValidPath(path)) {
      throw new ValidationException("InvalidPath");
    }
    FileModel fileModel = fileService.lookupPath(id.toString(), path, depth);
    return new ResponseEntity<>(fileModel, HttpStatus.OK);
  }

  // --dataset policies --
  @Override
  public ResponseEntity<PolicyResponse> addDatasetPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @Valid @RequestBody PolicyMemberRequest policyMember) {
    PolicyModel policy =
        iamService.addPolicyMember(
            getAuthenticatedInfo(),
            IamResourceType.DATASET,
            id,
            policyName,
            policyMember.getEmail());
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<PolicyResponse> retrieveDatasetPolicies(@PathVariable("id") UUID id) {
    List<PolicyModel> policies =
        iamService.retrievePolicies(getAuthenticatedInfo(), IamResourceType.DATASET, id);
    PolicyResponse response = new PolicyResponse().policies(policies);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<PolicyResponse> deleteDatasetPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @PathVariable("memberEmail") String memberEmail) {
    // member email can't be null since it is part of the URL
    if (!ValidationUtils.isValidEmail(memberEmail)) {
      throw new ValidationException("InvalidMemberEmail");
    }
    PolicyModel policy =
        iamService.deletePolicyMember(
            getAuthenticatedInfo(), IamResourceType.DATASET, id, policyName, memberEmail);
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
