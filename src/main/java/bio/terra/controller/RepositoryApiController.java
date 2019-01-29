package bio.terra.controller;

import bio.terra.controller.exception.ApiException;
import bio.terra.flight.StudyCreateFlight;
import bio.terra.model.*;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightResult;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.HashSet;
import java.util.Optional;

@Controller
public class RepositoryApiController implements RepositoryApi {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private HttpServletRequest request;

    @Autowired
    private Stairway stairway;

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

    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        // TODO: validation should happen at some point, either at job submission or when the @Valid annotation is used
        HashSet<String> seenTableNames = new HashSet<>();
        for (TableModel table : studyRequest.getSchema().getTables()) {
            String tableName = table.getName();
            if (seenTableNames.contains(tableName)) {
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }
            seenTableNames.add(tableName);
            HashSet<String> seenColumnNames = new HashSet<>();
            for (ColumnModel column : table.getColumns()) {
                String columnName = column.getName();
                if (seenColumnNames.contains(columnName)) {
                    return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
                }
                seenColumnNames.add(columnName);
            }
        }
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
