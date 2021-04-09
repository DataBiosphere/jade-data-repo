package bio.terra.app.controller;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.ValidationUtils;
import bio.terra.controller.DatasetsApi;
import bio.terra.model.AssetModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
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

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

@Controller
@Api(tags = {"datasets"})
public class DatasetsApiController implements DatasetsApi {

    private Logger logger = LoggerFactory.getLogger(DatasetsApiController.class);

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
            IngestRequestValidator ingestRequestValidator
    ) {
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
    public ResponseEntity<JobModel> createDataset(@Valid @RequestBody DatasetRequestModel datasetRequest) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        String jobId = datasetService.createDataset(datasetRequest, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<DatasetModel> retrieveDataset(@PathVariable("id") String id) {
        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASET, id, IamAction.READ_DATASET);
        return new ResponseEntity<>(datasetService.retrieveAvailableDatasetModel(UUID.fromString(id)), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> deleteDataset(@PathVariable("id") String id) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.DELETE);
        String jobId = datasetService.delete(id, userReq);
        // we can retrieve the job we just created
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<EnumerateDatasetModel> enumerateDatasets(
            @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
            @Valid @RequestParam(value = "sort", required = false, defaultValue = "created_date") String sort,
            @Valid @RequestParam(value = "direction", required = false, defaultValue = "asc") String direction,
            @Valid @RequestParam(value = "filter", required = false) String filter) {
        ControllerUtils.validateEnumerateParams(offset, limit, sort, direction);
        List<UUID> resources = iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASET);
        EnumerateDatasetModel esm = datasetService.enumerate(offset, limit, sort, direction, filter, resources);
        return new ResponseEntity<>(esm, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> ingestDataset(@PathVariable("id") String id,
                                                  @Valid @RequestBody IngestRequestModel ingest) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.INGEST_DATA);
        String jobId = datasetService.ingestDataset(id, ingest, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> addDatasetAssetSpecifications(@PathVariable("id") String id,
                                                  @Valid @RequestBody AssetModel asset) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.MANAGE_SCHEMA);
        String jobId = datasetService.addDatasetAssetSpecifications(id, asset, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> removeDatasetAssetSpecifications(@PathVariable("id") String id,
                                                                     @PathVariable("assetId") String assetId) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.MANAGE_SCHEMA);
        String jobId = datasetService.removeDatasetAssetSpecifications(id, assetId, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> applyDatasetDataDeletion(
        String id,
        @RequestBody @Valid DataDeletionRequest dataDeletionRequest) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        String jobId = datasetService.deleteTabularData(id, dataDeletionRequest, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    // -- dataset-file --
    @Override
    public ResponseEntity<JobModel> deleteFile(@PathVariable("id") String id,
                                               @PathVariable("fileid") String fileid) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.SOFT_DELETE);
        String jobId = fileService.deleteFile(id, fileid, userReq);
        // we can retrieve the job we just created
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> ingestFile(@PathVariable("id") String id,
                                               @Valid @RequestBody FileLoadModel ingestFile) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.INGEST_DATA);
        String jobId = fileService.ingestFile(id, ingestFile, userReq);
        // we can retrieve the job we just created
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> bulkFileLoad(@PathVariable("id") String id,
                                                 @Valid @RequestBody BulkLoadRequestModel bulkFileLoad) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        String jobId = fileService.ingestBulkFile(id, bulkFileLoad, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> bulkFileLoadArray(@PathVariable("id") String id,
                                                      @Valid @RequestBody BulkLoadArrayRequestModel bulkFileLoadArray) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        String jobId = fileService.ingestBulkFileArray(id, bulkFileLoadArray, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<FileModel> lookupFileById(
        @PathVariable("id") String id,
        @PathVariable("fileid") String fileid,
        @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {
        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASET, id, IamAction.READ_DATA);
        FileModel fileModel = fileService.lookupFile(id, fileid, depth);
        return new ResponseEntity<>(fileModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<FileModel> lookupFileByPath(
        @PathVariable("id") String id,
        @RequestParam(value = "path", required = true) String path,
        @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASET, id, IamAction.READ_DATA);
        if (!ValidationUtils.isValidPath(path)) {
            throw new ValidationException("InvalidPath");
        }
        FileModel fileModel = fileService.lookupPath(id, path, depth);
        return new ResponseEntity<>(fileModel, HttpStatus.OK);
    }

    // --dataset policies --
    @Override
    public ResponseEntity<PolicyResponse> addDatasetPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        PolicyModel policy = iamService.addPolicyMember(
            getAuthenticatedInfo(),
            IamResourceType.DATASET,
            UUID.fromString(id),
            policyName,
            policyMember.getEmail());
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveDatasetPolicies(@PathVariable("id") String id) {
        List<PolicyModel> policies = iamService.retrievePolicies(
            getAuthenticatedInfo(),
            IamResourceType.DATASET,
            UUID.fromString(id));
        PolicyResponse response = new PolicyResponse().policies(policies);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteDatasetPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @PathVariable("memberEmail") String memberEmail) {
        // member email can't be null since it is part of the URL
        if (!ValidationUtils.isValidEmail(memberEmail)) {
            throw new ValidationException("InvalidMemberEmail");
        }
        PolicyModel policy = iamService.deletePolicyMember(
            getAuthenticatedInfo(),
            IamResourceType.DATASET,
            UUID.fromString(id),
            policyName,
            memberEmail);
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
