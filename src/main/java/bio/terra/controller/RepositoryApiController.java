package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.controller.exception.ValidationException;
import bio.terra.exception.BadRequestException;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.DatasetService;
import bio.terra.service.JobService;
import bio.terra.service.StudyService;
import bio.terra.validation.DatasetRequestValidator;
import bio.terra.validation.IngestRequestValidator;
import bio.terra.validation.StudyRequestValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Controller
public class RepositoryApiController implements RepositoryApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final JobService jobService;
    private final StudyRequestValidator studyRequestValidator;
    private final StudyService studyService;
    private final DatasetRequestValidator datasetRequestValidator;
    private final DatasetService datasetService;
    private final IngestRequestValidator ingestRequestValidator;

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
            IngestRequestValidator ingestRequestValidator,
            ApplicationConfiguration appConfig
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.jobService = jobService;
        this.studyRequestValidator = studyRequestValidator;
        this.studyService = studyService;
        this.datasetRequestValidator = datasetRequestValidator;
        this.datasetService = datasetService;
        this.ingestRequestValidator = ingestRequestValidator;
        this.appConfig = appConfig;
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(studyRequestValidator);
        binder.addValidators(datasetRequestValidator);
        binder.addValidators(ingestRequestValidator);
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
        if (!getRequest().isPresent()) {
            throw new BadRequestException("No valid request found.");
        }
        HttpServletRequest req = getRequest().get();
        String email = req.getHeader("oidc_claim_email");
        String token = req.getHeader("oidc_access_token");

        if (token == null) {
            token = req.getHeader("Authorization").substring("Bearer ".length());
        }
        if (email == null) {
            email = appConfig.getUserEmail();
        }
        return new AuthenticatedUserRequest(email, token);
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
            @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
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

        return new ResponseEntity<>(studyService.enumerate(offset, limit), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<JobModel> ingestStudy(@PathVariable("id") String id,
                                                @Valid @RequestBody IngestRequestModel ingest) {
        String jobId = studyService.ingestStudy(id, ingest);
        return jobService.retrieveJob(jobId);
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
            @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {

        List<DatasetSummaryModel> datasetSummaryModels = datasetService.enumerateDatasets(offset, limit);
        return new ResponseEntity<>(datasetSummaryModels, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DatasetModel> retrieveDataset(@PathVariable("id") String id) {
        DatasetModel datasetModel = datasetService.retrieveDataset(UUID.fromString(id));
        return new ResponseEntity<>(datasetModel, HttpStatus.OK);
    }

    // -- jobs --
    @Override
    public ResponseEntity<List<JobModel>> enumerateJobs(
            @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
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
}
