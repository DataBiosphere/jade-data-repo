package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.controller.exception.ValidationException;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.exception.UnauthorizedException;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.SnapshotSource;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyResponse;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.SnapshotService;
import bio.terra.service.FileService;
import bio.terra.service.JobService;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
import bio.terra.validation.SnapshotRequestValidator;
import bio.terra.validation.IngestRequestValidator;
import bio.terra.validation.PolicyMemberValidator;
import bio.terra.validation.DatasetRequestValidator;
import bio.terra.validation.ValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
    private final SamClientService samService;
    private final IngestRequestValidator ingestRequestValidator;
    private final FileService fileService;
    private final PolicyMemberValidator policyMemberValidator;

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
            SamClientService samService,
            IngestRequestValidator ingestRequestValidator,
            ApplicationConfiguration appConfig,
            FileService fileService,
            PolicyMemberValidator policyMemberValidator
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.jobService = jobService;
        this.datasetRequestValidator = datasetRequestValidator;
        this.datasetService = datasetService;
        this.snapshotRequestValidator = snapshotRequestValidator;
        this.snapshotService = snapshotService;
        this.samService = samService;
        this.ingestRequestValidator = ingestRequestValidator;
        this.appConfig = appConfig;
        this.fileService = fileService;
        this.policyMemberValidator = policyMemberValidator;
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(datasetRequestValidator);
        binder.addValidators(snapshotRequestValidator);
        binder.addValidators(ingestRequestValidator);
        binder.addValidators(policyMemberValidator);
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
        return AuthenticatedUserRequest.from(getRequest(), appConfig.getUserEmail());
    }

    // -- dataset --
    @Override
    public ResponseEntity<DatasetSummaryModel> createDataset(@Valid @RequestBody DatasetRequestModel datasetRequest) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATAREPO,
            appConfig.datarepoId(),
            SamClientService.DataRepoAction.CREATE_DATASET);
        return new ResponseEntity<>(datasetService.createDataset(datasetRequest, getAuthenticatedInfo()),
            HttpStatus.CREATED);
    }

    @Override
    public ResponseEntity<DatasetModel> retrieveDataset(@PathVariable("id") String id) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.READ_DATASET);
        return new ResponseEntity<>(datasetService.retrieveModel(UUID.fromString(id)), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DeleteResponseModel> deleteDataset(@PathVariable("id") String id) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.DELETE);
        return new ResponseEntity<>(datasetService.delete(UUID.fromString(id), getAuthenticatedInfo()), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<EnumerateDatasetModel> enumerateDatasets(
            @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
            @Valid @RequestParam(value = "sort", required = false, defaultValue = "created_date") String sort,
            @Valid @RequestParam(value = "direction", required = false, defaultValue = "asc") String direction,
            @Valid @RequestParam(value = "filter", required = false) String filter) {
        ControllerUtils.validateEnumerateParams(offset, limit, sort, direction);
        try {
            List<ResourceAndAccessPolicy> resources = samService.listAuthorizedResources(
                getAuthenticatedInfo(), SamClientService.ResourceType.DATASET);
            EnumerateDatasetModel esm = datasetService.enumerate(offset, limit, sort, direction, filter, resources);
            return new ResponseEntity<>(esm, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<JobModel> ingestDataset(@PathVariable("id") String id,
                                                @Valid @RequestBody IngestRequestModel ingest) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.INGEST_DATA);
        String jobId = datasetService.ingestDataset(id, ingest);
        return jobService.retrieveJob(jobId);
    }

    // -- dataset-file --
    @Override
    public ResponseEntity<JobModel> deleteFile(@PathVariable("id") String id,
                                               @PathVariable("fileid") String fileid) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.UPDATE_DATA);
        String jobId = fileService.deleteFile(id, fileid);
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<JobModel> ingestFile(@PathVariable("id") String id,
                                               @Valid @RequestBody FileLoadModel ingestFile) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.INGEST_DATA);
        String jobId = fileService.ingestFile(id, ingestFile);
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<FSObjectModel> lookupFileObjectById(@PathVariable("id") String id,
                                                @PathVariable("fileid") String fileid) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.READ_DATA);
        FSObjectModel fsObjectModel = fileService.lookupFile(id, fileid);
        return new ResponseEntity<>(fsObjectModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<FSObjectModel> lookupFileObjectByPath(@PathVariable("id") String id,
                                                @RequestParam(value = "path", required = true) String path) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASET,
            id,
            SamClientService.DataRepoAction.READ_DATA);
        if (!ValidationUtils.isValidPath(path)) {
            throw new ValidationException("InvalidPath");
        }
        FSObjectModel fsObjectModel = fileService.lookupPath(id, path);
        return new ResponseEntity<>(fsObjectModel, HttpStatus.OK);
    }

    // --dataset policies --
    @Override
    public ResponseEntity<PolicyResponse> addDatasetPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        try {
            PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(
                samService.addPolicyMember(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASET,
                    UUID.fromString(id),
                    policyName,
                    policyMember.getEmail())));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveDatasetPolicies(@PathVariable("id") String id) {
        try {
            PolicyResponse response = new PolicyResponse().policies(
                samService.retrievePolicies(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASET,
                    UUID.fromString(id)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteDatasetPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @PathVariable("memberEmail") String memberEmail) {
        // member email can't be null since it is part of the URL
        if (!ValidationUtils.isValidEmail(memberEmail))
            throw new ValidationException("InvalidMemberEmail");
        try {
            PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(
                samService.deletePolicyMember(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASET,
                    UUID.fromString(id),
                    policyName,
                    memberEmail)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            logger.error("got error from sam", ex);
            throw new InternalServerErrorException(ex);
        }
    }
    // -- snapshot --
    @Override
    public ResponseEntity<JobModel> createSnapshot(@Valid @RequestBody SnapshotRequestModel snapshotRequestModel) {
        Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequestModel);
        List<SnapshotSource> sources = snapshot.getSnapshotSources();
        List<SnapshotSource> unauthorized = new ArrayList();
        sources.forEach(source -> {
                if (!samService.isAuthorized(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASET,
                    source.getDataset().getId().toString(),
                    SamClientService.DataRepoAction.CREATE_DATASNAPSHOT)) {
                    unauthorized.add(source);
                }
            }
        );
        if (unauthorized.isEmpty()) {
            String jobId = snapshotService.createSnapshot(snapshotRequestModel, getAuthenticatedInfo());
            return jobService.retrieveJob(jobId);
        }
        throw new UnauthorizedException(
            "User is not authorized to create snapshots for these datasets " + unauthorized);
    }

    @Override
    public ResponseEntity<JobModel> deleteSnapshot(@PathVariable("id") String id) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASNAPSHOT,
            id,
            SamClientService.DataRepoAction.DELETE);
        String jobId = snapshotService.deleteSnapshot(UUID.fromString(id), getAuthenticatedInfo());
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<EnumerateSnapshotModel> enumerateSnapshots(
        @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
        @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
        @Valid @RequestParam(value = "sort", required = false, defaultValue = "created_date") String sort,
        @Valid @RequestParam(value = "direction", required = false, defaultValue = "asc") String direction,
        @Valid @RequestParam(value = "filter", required = false) String filter) {
        ControllerUtils.validateEnumerateParams(offset, limit, sort, direction);
        try {
            List<ResourceAndAccessPolicy> resources = samService.listAuthorizedResources(
                getAuthenticatedInfo(), SamClientService.ResourceType.DATASNAPSHOT);
            EnumerateSnapshotModel edm = snapshotService.enumerateSnapshots(offset, limit, sort,
                direction, filter, resources);
            return new ResponseEntity<>(edm, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<SnapshotModel> retrieveSnapshot(@PathVariable("id") String id) {
        samService.verifyAuthorization(
            getAuthenticatedInfo(),
            SamClientService.ResourceType.DATASNAPSHOT,
            id,
            SamClientService.DataRepoAction.READ_DATA);
        SnapshotModel snapshotModel = snapshotService.retrieveSnapshot(UUID.fromString(id));
        return new ResponseEntity<>(snapshotModel, HttpStatus.OK);
    }

    // --snapshot policies --
    @Override
    public ResponseEntity<PolicyResponse> addSnapshotPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        try {
            PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(
                samService.addPolicyMember(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASNAPSHOT,
                    UUID.fromString(id),
                    policyName,
                    policyMember.getEmail())));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveSnapshotPolicies(@PathVariable("id") String id) {
        try {
            PolicyResponse response = new PolicyResponse().policies(
                samService.retrievePolicies(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASNAPSHOT,
                    UUID.fromString(id)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteSnapshotPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @PathVariable("memberEmail") String memberEmail) {
        // member email can't be null since it is part of the URL
        if (!ValidationUtils.isValidEmail(memberEmail))
            throw new ValidationException("InvalidMemberEmail");
        try {
            PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(
                samService.deletePolicyMember(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.DATASNAPSHOT,
                    UUID.fromString(id),
                    policyName,
                    memberEmail)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            logger.error("got error from sam", ex);
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<UserStatusInfo> user() {
        try {
            UserStatusInfo info = samService.getUserInfo(getAuthenticatedInfo());
            return new ResponseEntity<>(info, HttpStatus.OK);
        } catch (ApiException ex) {
            logger.error("got error from sam", ex);
            throw new InternalServerErrorException(ex);
        }
    }

    // -- jobs --
    @Override
    public ResponseEntity<List<JobModel>> enumerateJobs(
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        validiateOffsetAndLimit(offset, limit);
        return jobService.enumerateJobs(offset, limit);
    }

    @Override
    public ResponseEntity<JobModel> retrieveJob(@PathVariable("id") String id) {
        return jobService.retrieveJob(id);
    }

    @Override
    public ResponseEntity<Object> retrieveJobResult(@PathVariable("id") String id) {
        return jobService.retrieveJobResultResponse(id);
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
