package bio.terra.controller;

import bio.terra.service.JobMapKeys;
import bio.terra.service.JobService;
import bio.terra.controller.exception.ApiException;
import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.metadata.Study;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.validation.StudyRequestValidator;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.validation.StudyRequestValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Controller
public class RepositoryApiController implements RepositoryApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final Stairway stairway;
    private final JobService jobService;
    private final StudyRequestValidator studyRequestValidator;
    private final StudyDao studyDao;

    @Autowired
    public RepositoryApiController(
            ObjectMapper objectMapper,
            HttpServletRequest request,
            Stairway stairway,
            JobService jobService,
            StudyRequestValidator studyRequestValidator,
            StudyDao studyDao
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.stairway = stairway;
        this.jobService = jobService;
        this.studyRequestValidator = studyRequestValidator;
        this.studyDao = studyDao;
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(studyRequestValidator);
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorModel> handleAsyncException(ApiException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorModel> handleValidationException(ValidationException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(FlightNotFoundException.class)
    public ResponseEntity<ErrorModel> handleFlightNotFoundException(FlightNotFoundException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.BAD_REQUEST);
    }

    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        FlightMap flightMap = new FlightMap();
        flightMap.put("request", studyRequest);
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        StudySummaryModel studySummary = getResponse(flightId, StudySummaryModel.class);
        return new ResponseEntity<>(studySummary, HttpStatus.CREATED);
    }

    @ExceptionHandler(StudyNotFoundException.class)
    public ResponseEntity<ErrorModel> handleStudyNotFoundException(StudyNotFoundException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorModel> handleStudyNotFoundException(IllegalArgumentException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.BAD_REQUEST);
    }

    public ResponseEntity<StudyModel> retrieveStudy(@PathVariable("id") String id) {
        Study study = studyDao.retrieve(UUID.fromString(id));
        StudyModel studyModel = StudyJsonConversion.studyModelFromStudy(study);
        return new ResponseEntity<>(studyModel, HttpStatus.OK);
    }

    public ResponseEntity<List<JobModel>> enumerateJobs(Integer offset, Integer limit) {
        return jobService.enumerateJobs(offset, limit);
    }

    public ResponseEntity<JobModel> retrieveJob(String jobId) {
        return jobService.retrieveJob(jobId);
    }

    public ResponseEntity<Object> retrieveJobResult(String jobId) {
        return jobService.retrieveJobResult(jobId);
    }

    private <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        if (result.getFlightStatus() == FlightStatus.SUCCESS) {
            if (result.getResultMap().isPresent()) {
                FlightMap resultMap = result.getResultMap().get();
                return resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass);
            }
            // It should not happen that we have success and no result map
            // This is probably not the right exception, but we will replace this with
            // the async stuff and the problem will go away.
            throw new ApiException("Successful job, but no response!");
        }

        String message = "Could not complete flight";
        Optional<String> optErrorMessage = result.getErrorMessage();
        if (optErrorMessage.isPresent()) {
            message = message + ": " + optErrorMessage.get();
        }
        throw new ApiException(message);
    }
}
