package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.ValidationUtils;
import bio.terra.controller.RepositoryApi;
import bio.terra.model.AssetModel;
import bio.terra.model.Attribute;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.Cohort;
import bio.terra.model.CohortCreateInfo;
import bio.terra.model.CohortList;
import bio.terra.model.CohortUpdateInfo;
import bio.terra.model.ConceptSet;
import bio.terra.model.ConceptSetCreateInfo;
import bio.terra.model.ConceptSetList;
import bio.terra.model.ConceptSetUpdateInfo;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigListModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.CountQuery;
import bio.terra.model.Criteria;
import bio.terra.model.CriteriaGroup;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataType;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DisplayHint;
import bio.terra.model.DisplayHintDisplayHint;
import bio.terra.model.DisplayHintEnum;
import bio.terra.model.DisplayHintEnumEnumHintValues;
import bio.terra.model.DisplayHintList;
import bio.terra.model.DisplayHintNumericRange;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.HintQuery;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.Instance;
import bio.terra.model.InstanceCount;
import bio.terra.model.InstanceCountList;
import bio.terra.model.InstanceHierarchyFields;
import bio.terra.model.InstanceList;
import bio.terra.model.InstanceRelationshipFields;
import bio.terra.model.JobModel;
import bio.terra.model.Literal;
import bio.terra.model.LiteralValueUnion;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.Query;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.model.ValueDisplay;
import bio.terra.service.configuration.ConfigurationService;
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
import bio.terra.service.iam.PolicyMemberValidator;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
public class RepositoryApiController implements RepositoryApi {

    private Logger logger = LoggerFactory.getLogger(RepositoryApiController.class);

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final JobService jobService;
    private final DatasetRequestValidator datasetRequestValidator;
    private final DatasetService datasetService;
    private final SnapshotRequestValidator snapshotRequestValidator;
    private final SnapshotService snapshotService;
    private final IamService iamService;
    private final IngestRequestValidator ingestRequestValidator;
    private final FileService fileService;
    private final PolicyMemberValidator policyMemberValidator;
    private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
    private final ConfigurationService configurationService;
    private final AssetModelValidator assetModelValidator;

    // needed for local testing w/o proxy
    private final ApplicationConfiguration appConfig;

    @Autowired
    public RepositoryApiController(
            ObjectMapper objectMapper,
            HttpServletRequest request,
            JobService jobService,
            DatasetRequestValidator datasetRequestValidator,
            DatasetService datasetService,
            SnapshotRequestValidator snapshotRequestValidator,
            SnapshotService snapshotService,
            IamService iamService,
            IngestRequestValidator ingestRequestValidator,
            ApplicationConfiguration appConfig,
            FileService fileService,
            PolicyMemberValidator policyMemberValidator,
            AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
            ConfigurationService configurationService,
            AssetModelValidator assetModelValidator
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.jobService = jobService;
        this.datasetRequestValidator = datasetRequestValidator;
        this.datasetService = datasetService;
        this.snapshotRequestValidator = snapshotRequestValidator;
        this.snapshotService = snapshotService;
        this.iamService = iamService;
        this.ingestRequestValidator = ingestRequestValidator;
        this.appConfig = appConfig;
        this.fileService = fileService;
        this.policyMemberValidator = policyMemberValidator;
        this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
        this.configurationService = configurationService;
        this.assetModelValidator = assetModelValidator;
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(datasetRequestValidator);
        binder.addValidators(snapshotRequestValidator);
        binder.addValidators(ingestRequestValidator);
        binder.addValidators(policyMemberValidator);
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
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.EDIT_DATASET);
        String jobId = datasetService.addDatasetAssetSpecifications(id, asset, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<JobModel> removeDatasetAssetSpecifications(@PathVariable("id") String id,
                                                                     @PathVariable("assetId") String assetId) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.EDIT_DATASET);
        String jobId = datasetService.removeDatasetAssetSpecifications(id, assetId, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    // -- dataset-file --
    @Override
    public ResponseEntity<JobModel> deleteFile(@PathVariable("id") String id,
                                               @PathVariable("fileid") String fileid) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASET, id, IamAction.UPDATE_DATA);
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
    // -- snapshot --
    private List<UUID> getUnauthorizedSources(
        List<UUID> snapshotSourceDatasetIds, AuthenticatedUserRequest userReq) {
        return snapshotSourceDatasetIds
            .stream()
            .filter(sourceId -> !iamService.isAuthorized(
                userReq,
                IamResourceType.DATASET,
                sourceId.toString(),
                IamAction.CREATE_DATASNAPSHOT))
            .collect(Collectors.toList());
    }

    @Override
    public ResponseEntity<JobModel> createSnapshot(@Valid @RequestBody SnapshotRequestModel snapshotRequestModel) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        List<UUID> snapshotSourceDatasetIds =
            snapshotService.getSourceDatasetIdsFromSnapshotRequest(snapshotRequestModel);
        // TODO auth should be put into flight?
        List<UUID> unauthorized = getUnauthorizedSources(snapshotSourceDatasetIds, userReq);
        if (unauthorized.isEmpty()) {
            String jobId = snapshotService.createSnapshot(snapshotRequestModel, userReq);
            // we can retrieve the job we just created
            return jobToResponse(jobService.retrieveJob(jobId, userReq));
        }
        throw new IamUnauthorizedException(
            "User is not authorized to create snapshots for these datasets " + unauthorized);
    }

    @Override
    public ResponseEntity<JobModel> deleteSnapshot(@PathVariable("id") String id) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        iamService.verifyAuthorization(userReq, IamResourceType.DATASNAPSHOT, id, IamAction.DELETE);
        String jobId = snapshotService.deleteSnapshot(UUID.fromString(id), userReq);
        // we can retrieve the job we just created
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<EnumerateSnapshotModel> enumerateSnapshots(
        @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
        @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
        @Valid @RequestParam(value = "sort", required = false, defaultValue = "created_date") String sort,
        @Valid @RequestParam(value = "direction", required = false, defaultValue = "asc") String direction,
        @Valid @RequestParam(value = "filter", required = false) String filter) {
        ControllerUtils.validateEnumerateParams(offset, limit, sort, direction);
        List<UUID> resources = iamService.listAuthorizedResources(
            getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT);
        EnumerateSnapshotModel edm = snapshotService.enumerateSnapshots(offset, limit, sort,
            direction, filter, resources);
        return new ResponseEntity<>(edm, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<SnapshotModel> retrieveSnapshot(@PathVariable("id") String id) {
        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
        SnapshotModel snapshotModel = snapshotService.retrieveAvailableSnapshotModel(UUID.fromString(id));
        return new ResponseEntity<>(snapshotModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<FileModel> lookupSnapshotFileById(
        @PathVariable("id") String id,
        @PathVariable("fileid") String fileid,
        @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
        FileModel fileModel = fileService.lookupSnapshotFile(id, fileid, depth);
        return new ResponseEntity<>(fileModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<FileModel> lookupSnapshotFileByPath(
        @PathVariable("id") String id,
        @RequestParam(value = "path", required = true) String path,
        @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
        if (!ValidationUtils.isValidPath(path)) {
            throw new ValidationException("InvalidPath");
        }
        FileModel fileModel = fileService.lookupSnapshotPath(id, path, depth);
        return new ResponseEntity<>(fileModel, HttpStatus.OK);
    }


    // --snapshot policies --
    @Override
    public ResponseEntity<PolicyResponse> addSnapshotPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        PolicyModel policy = iamService.addPolicyMember(
            getAuthenticatedInfo(),
            IamResourceType.DATASNAPSHOT,
            UUID.fromString(id),
            policyName,
            policyMember.getEmail());
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> applyDatasetDataDeletion(
        String id,
        @RequestBody @Valid DataDeletionRequest dataDeletionRequest) {
        AuthenticatedUserRequest userReq = getAuthenticatedInfo();
        String jobId = datasetService.deleteTabularData(id, dataDeletionRequest, userReq);
        return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveSnapshotPolicies(@PathVariable("id") String id) {
        PolicyResponse response = new PolicyResponse().policies(
            iamService.retrievePolicies(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, UUID.fromString(id)));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteSnapshotPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @PathVariable("memberEmail") String memberEmail) {
        // member email can't be null since it is part of the URL
        if (!ValidationUtils.isValidEmail(memberEmail)) {
            throw new ValidationException("InvalidMemberEmail");
        }

        PolicyModel policy = iamService.deletePolicyMember(
            getAuthenticatedInfo(),
            IamResourceType.DATASNAPSHOT,
            UUID.fromString(id),
            policyName,
            memberEmail);
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<UserStatusInfo> user() {
        UserStatusInfo info = iamService.getUserInfo(getAuthenticatedInfo());
        return new ResponseEntity<>(info, HttpStatus.OK);
    }

    // -- jobs --
    private ResponseEntity<JobModel> jobToResponse(JobModel job) {
        if (job.getJobStatus() == JobModel.JobStatusEnum.RUNNING) {
            return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .header("Location", String.format("/api/repository/v1/jobs/%s", job.getId()))
                .body(job);
        } else {
            return ResponseEntity
                .status(HttpStatus.OK)
                .header("Location", String.format("/api/repository/v1/jobs/%s/result", job.getId()))
                .body(job);
        }
    }


    @Override
    public ResponseEntity<List<JobModel>> enumerateJobs(
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        validiateOffsetAndLimit(offset, limit);
        List<JobModel> results = jobService.enumerateJobs(offset, limit, getAuthenticatedInfo());
        return new ResponseEntity<>(results, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> retrieveJob(@PathVariable("id") String id) {
        JobModel job = jobService.retrieveJob(id, getAuthenticatedInfo());
        return jobToResponse(job);
    }

    @Override
    public ResponseEntity<Object> retrieveJobResult(@PathVariable("id") String id) {
        JobService.JobResultWithStatus<Object> resultHolder =
            jobService.retrieveJobResult(id, Object.class, getAuthenticatedInfo());
        return ResponseEntity.status(resultHolder.getStatusCode()).body(resultHolder.getResult());
    }

    @Override
    public ResponseEntity<Void> deleteJob(@PathVariable("id") String id) {
        jobService.releaseJob(id, getAuthenticatedInfo());
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @Override
    public ResponseEntity<ConfigModel> getConfig(@PathVariable("name") String name) {
        ConfigModel configModel = configurationService.getConfig(name);
        return new ResponseEntity<>(configModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ConfigListModel> getConfigList() {
        ConfigListModel configModelList = configurationService.getConfigList();
        return new ResponseEntity<>(configModelList, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Void> resetConfig() {
        configurationService.reset();
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @Override
    public ResponseEntity<ConfigListModel> setConfigList(@Valid @RequestBody ConfigGroupModel configModel) {
        ConfigListModel configModelList = configurationService.setConfig(configModel);
        return new ResponseEntity<>(configModelList, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Void> setFault(@PathVariable("name") String name,
                                         @Valid @RequestParam(value = "enable", required = false, defaultValue = "true")
                                             Boolean enable) {
        configurationService.setFault(name, enable);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @Override
    public ResponseEntity<CohortList> listCohorts(@RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
                                                  @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        CohortList response = new CohortList();
        response.add(generateStubCohort());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Cohort> createCohort(CohortCreateInfo cohortCreateInfo) {
        return new ResponseEntity<>(generateStubCohort(), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Cohort> getCohort(String cohortId) {
        Cohort response = generateStubCohort();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Cohort> updateCohort(String cohortId, CohortUpdateInfo cohortUpdateInfo) {
        return new ResponseEntity<>(generateStubCohort(), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Void> deleteCohort(String cohortId) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<InstanceCountList> countInstances(String entityName, CountQuery countQuery) {
        // Stub values based on what the API synthetic data returns in the other API - not representative of life
        List<ValueDisplay> yearsOfBirth = new ArrayList<>();
        for (long i = 1909; i <= 1983; i++) {
            yearsOfBirth.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(i))));
        }

        // Stub values based on what the API synthetic data returns in the other API - not representative of life
        List<ValueDisplay> races = new ArrayList<>();
        races.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(0L))).display("No matching concept"));
        races.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8516L))).display("Black or African American"));
        races.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8527L))).display("White"));

        // Stub values based on what the API synthetic data returns in the other API - not representative of life
        List<ValueDisplay> genders = new ArrayList<>();
        genders.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8507L))).display("MALE"));
        genders.add(new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8532L))).display("FEMALE"));

        return new ResponseEntity<>(
            new InstanceCountList()
                .sql("SELECT p.gender AS gender, p.race AS race, COUNT(p.id) AS t_count, p.t_display_gender AS t_display_gender, p.t_display_race AS t_display_race, p.year_of_birth AS year_of_birth FROM `broad-tanagra-dev.cmssynpuf_index_011523`.person AS p GROUP BY p.gender, p.t_display_gender, p.race, p.t_display_race, p.year_of_birth ORDER BY p.gender ASC, p.t_display_gender ASC, p.race ASC, p.t_display_race ASC, p.year_of_birth ASC")
                .instanceCounts(new ArrayList<>(createInstanceCountsYearGenderRace(genders, races, yearsOfBirth))),
            HttpStatus.OK);
    }

    @Override
    public ResponseEntity<InstanceList> queryInstances(String entityName, Query query) {
        List<Instance> instances = new ArrayList<>();
        List<InstanceRelationshipFields> relationshipFields = new ArrayList<>();
        relationshipFields.add(new InstanceRelationshipFields().relatedEntity("person").hierarchy("standard").count(1970071));
        relationshipFields.add(new InstanceRelationshipFields().relatedEntity("person").count(0));

        List<InstanceHierarchyFields> hierarchyFields = new ArrayList<>();
        hierarchyFields.add(new InstanceHierarchyFields().hierarchy("standard").path("").numChildren(29));

        Map<String, ValueDisplay> attributes = new HashMap<>();
        attributes.put("standard_concept", new ValueDisplay().display("Standard").value(new Literal().dataType(DataType.STRING).valueUnion(new LiteralValueUnion().stringVal("S"))));
        attributes.put("vocabulary", new ValueDisplay().display("Systematic Nomenclature of Medicine - Clinical Terms (IHTSDO)").value(new Literal().dataType(DataType.STRING).valueUnion(new LiteralValueUnion().stringVal("SNOMED"))));
        attributes.put("name", new ValueDisplay().value(new Literal().dataType(DataType.STRING).valueUnion(new LiteralValueUnion().stringVal("Clinical finding"))));
        attributes.put("concept_code", new ValueDisplay().value(new Literal().dataType(DataType.STRING).valueUnion(new LiteralValueUnion().stringVal("404684003"))));
        attributes.put("id", new ValueDisplay().value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(441840L))));



        instances.add(new Instance().relationshipFields(relationshipFields).hierarchyFields(hierarchyFields).attributes(attributes));
        return new ResponseEntity<>(new InstanceList()
            .sql("SELECT c.concept_code AS concept_code, c.id AS id, c.name AS name, c.standard_concept AS standard_concept, c.t_count_person AS t_count_person, c.t_count_person_standard AS t_count_person_standard, c.t_display_standard_concept AS t_display_standard_concept, c.t_display_vocabulary AS t_display_vocabulary, c.t_standard_num_children AS t_standard_num_children, c.t_standard_path AS t_standard_path, c.vocabulary AS vocabulary FROM `broad-tanagra-dev.cmssynpuf_index_011523`.condition AS c WHERE (c.t_standard_path IS NOT NULL AND c.t_standard_path = '') ORDER BY c.t_count_person_standard DESC LIMIT 250")
            .instances(instances), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ConceptSetList> listConceptSets(@RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
                                                          @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        ConceptSetList response = new ConceptSetList();
        response.add(generateStubConceptSet());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ConceptSet> createConceptSet(ConceptSetCreateInfo conceptSetCreateInfo) {
        return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ConceptSet> getConceptSet(String ConceptSetId) {
        return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ConceptSet> updateConceptSet(String conceptSetId, ConceptSetUpdateInfo conceptSetUpdateInfo) {
        return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Void> deleteConceptSet(String conceptSetId) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DisplayHintList> queryHints(String entityId, HintQuery hintQuery) {
        DisplayHintList response = new DisplayHintList()
            .addDisplayHintsItem(new DisplayHint()
                .attribute(new Attribute()
                    .name("gender")
                    .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                    .dataType(DataType.INT64)
                ).displayHint(new DisplayHintDisplayHint()
                    .enumHint(new DisplayHintEnum().enumHintValues(Arrays.asList(
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("FEMALE").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8532L))))
                            .count(1292861),
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("MALE").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8507L))))
                            .count(1033995)
                    )))))
            .addDisplayHintsItem(new DisplayHint()
                .attribute(new Attribute()
                    .name("race")
                    .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                    .dataType(DataType.INT64)
                ).displayHint(new DisplayHintDisplayHint()
                    .enumHint(new DisplayHintEnum().enumHintValues(Arrays.asList(
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("Black or African American").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8516L))))
                            .count(247723),
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("No matching concept").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(0L))))
                            .count(152425),
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("White").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(8527L))))
                            .count(1926708)
                    ))))
            )
            .addDisplayHintsItem(new DisplayHint()
                .attribute(new Attribute()
                    .name("ethnicity")
                    .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                    .dataType(DataType.INT64)
                ).displayHint(new DisplayHintDisplayHint()
                    .enumHint(new DisplayHintEnum().enumHintValues(Arrays.asList(
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("Hispanic or Latino").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(38003563L))))
                            .count(54453),
                        new DisplayHintEnumEnumHintValues()
                            .enumVal(new ValueDisplay().display("Not Hispanic or Latino").value(new Literal().dataType(DataType.INT64).valueUnion(new LiteralValueUnion().int64Val(38003564L))))
                            .count(2272403)
                    ))))
            )
            .addDisplayHintsItem(new DisplayHint()
                .attribute(new Attribute()
                    .name("year_of_birth")
                    .type(Attribute.TypeEnum.SIMPLE)
                    .dataType(DataType.INT64)
                ).displayHint(new DisplayHintDisplayHint().numericRangeHint(new DisplayHintNumericRange().min(1909.0D).max(1983.0D)))
            );

        return new ResponseEntity<>(new DisplayHintList(), HttpStatus.NOT_IMPLEMENTED);
    }

    // Our stubs have the same count for everything
    private List<InstanceCount> createInstanceCountsYearGenderRace(List<ValueDisplay> genders, List<ValueDisplay> races, List<ValueDisplay> yearsOfBirth) {
        List<InstanceCount> instanceCounts = new ArrayList<>();
         genders.forEach((ValueDisplay gender) ->
            races.forEach(race ->
                yearsOfBirth.forEach((ValueDisplay yearOfBirth) -> {
                    HashMap<String, ValueDisplay> attributes = new HashMap<>();
                    attributes.put("gender", new ValueDisplay().value(gender.getValue()).display(gender.getDisplay()));
                    attributes.put("race", new ValueDisplay().value(race.getValue()).display(race.getDisplay()));
                    attributes.put("year_of_birth", new ValueDisplay().value(yearOfBirth.getValue()).display(yearOfBirth.getDisplay()));
                    instanceCounts.add(new InstanceCount().count(20000).attributes(attributes));
                })
            )
        );
        return instanceCounts;
    }

    private Cohort generateStubCohort() {
        List<CriteriaGroup> criteriaGroups = new java.util.ArrayList<>();
        List<Criteria> criteria = new java.util.ArrayList<>();
        criteria.add(new Criteria()
            .id("Q1Ng787e")
            .displayName("")
            .pluginName("classification")
            .selectionData("{\"selected\":[{\"key\":4047779,\"name\":\"Disorder by body site\"}]}")
            .uiConfig("{\"type\":\"classification\",\"id\":\"tanagra-conditions\",\"title\":\"Condition\",\"conceptSet\":true,\"category\":\"Domains\",\"columns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Concept name\"},{\"key\":\"id\",\"width\":100,\"title\":\"Concept ID\"},{\"key\":\"standard_concept\",\"width\":120,\"title\":\"Source/standard\"},{\"key\":\"vocabulary_t_value\",\"width\":120,\"title\":\"Vocab\"},{\"key\":\"concept_code\",\"width\":120,\"title\":\"Code\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"hierarchyColumns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Condition\"},{\"key\":\"id\",\"width\":120,\"title\":\"Concept ID\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"occurrence\":\"condition_occurrence\",\"classification\":\"condition\"}"
            ));
        criteriaGroups.add(new CriteriaGroup()
            .id("A3FJlc87")
            .displayName("")
            .criteria(
                criteria
            ));
        return new Cohort()
            .id("42VHg43VxA")
            .displayName("My Cohort")
            .criteriaGroups(criteriaGroups);
    }

    private ConceptSet generateStubConceptSet() {
        Criteria criteria = new Criteria()
            .id("DrX3z0V9")
            .displayName("")
            .pluginName("classification")
            .selectionData("{\"selected\":[{\"key\":441840,\"name\":\"Clinical finding\"}]}")
            .uiConfig("{\"type\":\"classification\",\"id\":\"tanagra-conditions\",\"title\":\"Condition\",\"conceptSet\":true,\"category\":\"Domains\",\"columns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Concept name\"},{\"key\":\"id\",\"width\":100,\"title\":\"Concept ID\"},{\"key\":\"standard_concept\",\"width\":120,\"title\":\"Source/standard\"},{\"key\":\"vocabulary_t_value\",\"width\":120,\"title\":\"Vocab\"},{\"key\":\"concept_code\",\"width\":120,\"title\":\"Code\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"hierarchyColumns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Condition\"},{\"key\":\"id\",\"width\":120,\"title\":\"Concept ID\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"occurrence\":\"condition_occurrence\",\"classification\":\"condition\"}"
            );
        return new ConceptSet()
            .id("Mu6OSPzPdp")
            .entity("condition_occurrence")
            .created(OffsetDateTime.now())
            .lastModified(OffsetDateTime.now())
            .criteria(criteria)
            .createdBy("authentication-disabled");
    }


    private void validiateOffsetAndLimit(Integer offset, Integer limit) {
        String errors = "";
        offset = (offset == null) ? offset = 0 : offset;
        if (offset < 0) {
            errors = "Offset must be greater than or equal to 0.";
        }

        limit =  (limit == null) ? limit = 10 : limit;
        if (limit < 1) {
            errors += " Limit must be greater than or equal to 1.";
        }
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }

    }

}
