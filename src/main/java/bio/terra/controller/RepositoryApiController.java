package bio.terra.controller;

import bio.terra.controller.exception.ApiException;
import bio.terra.controller.exception.ValidationException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.model.*;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightResult;
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
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Optional;

@Controller
public class RepositoryApiController implements RepositoryApi {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private HttpServletRequest request;

    @Autowired
    private Stairway stairway;

    @Autowired
    private StudyRequestValidator studyRequestValidator;

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

    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        FlightMap flightMap = new FlightMap();
        flightMap.put("request", studyRequest);
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        StudySummaryModel studySummary = getResponse(flightId, StudySummaryModel.class);
        return new ResponseEntity<>(studySummary, HttpStatus.CREATED);
    }

    public <T> T getResponse(String flightId, Class<T> resultClass) {
        FlightResult result = stairway.getResult(flightId);
        if (result.isSuccess()) {
            FlightMap resultMap = result.getResultMap();
            return resultMap.get("response", resultClass);
        } else {
            String message = "Could not complete flight";
            Optional<Throwable> optThrowable = result.getThrowable();
            if (optThrowable.isPresent()) {
                Throwable throwable = optThrowable.get();
                throwable.printStackTrace();
                message = throwable.getMessage();
                throw new ApiException(message, throwable);
            }
            throw new ApiException(message);
        }
    }
}
