package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.controller.exception.ValidationException;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyResponse;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.DatasetService;
import bio.terra.service.FileService;
import bio.terra.service.JobService;
import bio.terra.service.SamClientService;
import bio.terra.service.StudyService;
import bio.terra.validation.DatasetRequestValidator;
import bio.terra.validation.IngestRequestValidator;
import bio.terra.validation.PolicyMemberValidator;
import bio.terra.validation.StudyRequestValidator;
import bio.terra.validation.ValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
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

@Controller
public class RepositoryApiController implements RepositoryApi {

    private Logger logger = LoggerFactory.getLogger(RepositoryApiController.class);

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final JobService jobService;
    private final StudyRequestValidator studyRequestValidator;
    private final StudyService studyService;
    private final DatasetRequestValidator datasetRequestValidator;
    private final DatasetService datasetService;
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
            StudyRequestValidator studyRequestValidator,
            StudyService studyService,
            DatasetRequestValidator datasetRequestValidator,
            DatasetService datasetService,
            SamClientService samService,
            IngestRequestValidator ingestRequestValidator,
            ApplicationConfiguration appConfig,
            FileService fileService,
            PolicyMemberValidator policyMemberValidator
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.jobService = jobService;
        this.studyRequestValidator = studyRequestValidator;
        this.studyService = studyService;
        this.datasetRequestValidator = datasetRequestValidator;
        this.datasetService = datasetService;
        this.samService = samService;
        this.ingestRequestValidator = ingestRequestValidator;
        this.appConfig = appConfig;
        this.fileService = fileService;
        this.policyMemberValidator = policyMemberValidator;
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(studyRequestValidator);
        binder.addValidators(datasetRequestValidator);
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

    // -- study --
    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        return new ResponseEntity<>(studyService.createStudy(studyRequest, getAuthenticatedInfo()), HttpStatus.CREATED);
    }

    public ResponseEntity<StudyModel> retrieveStudy(@PathVariable("id") String id) {
        return new ResponseEntity<>(studyService.retrieve(UUID.fromString(id)), HttpStatus.OK);
    }

    public ResponseEntity<DeleteResponseModel> deleteStudy(@PathVariable("id") String id) {
        return new ResponseEntity<>(studyService.delete(UUID.fromString(id), getAuthenticatedInfo()), HttpStatus.OK);
    }

    public ResponseEntity<List<StudySummaryModel>> enumerateStudies(
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        validiateOffsetAndLimit(offset, limit);
        return new ResponseEntity<>(studyService.enumerate(offset, limit), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> ingestStudy(@PathVariable("id") String id,
                                                @Valid @RequestBody IngestRequestModel ingest) {
        String jobId = studyService.ingestStudy(id, ingest);
        return jobService.retrieveJob(jobId);
    }

    // -- study-file --
    @Override
    public ResponseEntity<JobModel> deleteFile(@PathVariable("id") String id,
                                               @PathVariable("fileid") String fileid) {
        String jobId = fileService.deleteFile(id, fileid);
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<JobModel> ingestFile(@PathVariable("id") String id,
                                               @Valid @RequestBody FileLoadModel ingestFile) {
        String jobId = fileService.ingestFile(id, ingestFile);
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<FileModel> lookupfile(@PathVariable("id") String id,
                                                @PathVariable("fileid") String fileid) {
        FileModel fileModel = fileService.lookupFile(id, fileid);
        return new ResponseEntity<>(fileModel, HttpStatus.OK);
    }

    // --study policies --
    @Override
    public ResponseEntity<PolicyResponse> addStudyPolicyMember(
        @PathVariable("id") String id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        try {
            PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(
                samService.addPolicyMember(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.STUDY,
                    UUID.fromString(id),
                    policyName,
                    policyMember.getEmail())));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveStudyPolicies(@PathVariable("id") String id) {
        try {
            PolicyResponse response = new PolicyResponse().policies(
                samService.retrievePolicies(
                    getAuthenticatedInfo(),
                    SamClientService.ResourceType.STUDY,
                    UUID.fromString(id)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteStudyPolicyMember(
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
                    SamClientService.ResourceType.STUDY,
                    UUID.fromString(id),
                    policyName,
                    memberEmail)));
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (ApiException ex) {
            logger.error("got error from sam", ex);
            throw new InternalServerErrorException(ex);
        }
    }
    // -- dataset --
    @Override
    public ResponseEntity<JobModel> createDataset(@Valid @RequestBody DatasetRequestModel dataset) {
        String jobId = datasetService.createDataset(dataset, getAuthenticatedInfo());
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<JobModel> deleteDataset(@PathVariable("id") String id) {
        String jobId = datasetService.deleteDataset(UUID.fromString(id), getAuthenticatedInfo());
        return jobService.retrieveJob(jobId);
    }

    @Override
    public ResponseEntity<List<DatasetSummaryModel>> enumerateDatasets(
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        validiateOffsetAndLimit(offset, limit);
        List<DatasetSummaryModel> datasetSummaryModels = datasetService.enumerateDatasets(offset, limit);
        return new ResponseEntity<>(datasetSummaryModels, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DatasetModel> retrieveDataset(@PathVariable("id") String id) {
        DatasetModel datasetModel = datasetService.retrieveDataset(UUID.fromString(id));
        return new ResponseEntity<>(datasetModel, HttpStatus.OK);
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
