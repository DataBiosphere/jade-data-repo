package bio.terra.controller;

import bio.terra.controller.exception.ApiException;
import bio.terra.controller.exception.ValidationException;
import bio.terra.flight.study.create.StudyCreateFlight;
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

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final Stairway stairway;

    @Autowired
    public RepositoryApiController(ObjectMapper objectMapper, HttpServletRequest request, Stairway stairway) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.stairway = stairway;
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
        HashSet<String> seenTableNames = new HashSet<>();
        for (TableModel table : studyRequest.getSchema().getTables()) {
            String tableName = table.getName();
            if (seenTableNames.contains(tableName)) {
                throw new ValidationException("table names must be unique");
            }
            seenTableNames.add(tableName);
            HashSet<String> seenColumnNames = new HashSet<>();
            for (ColumnModel column : table.getColumns()) {
                String columnName = column.getName();
                if (seenColumnNames.contains(columnName)) {
                    throw new ValidationException("column names must be unique within a table");
                }
                seenColumnNames.add(columnName);
            }
        }
        boolean thereIsARootTable = false;
        for (AssetModel assetModel : studyRequest.getSchema().getAssets()) {
            // TODO: assert that there is at least one asset
            // TODO: assert that all strings in assetModel.getFollow() match relationships
            // TODO: assert that all asset names are unique
            // TODO: assert that all asset tables reference real tables
            // TODO: assert that all asset table columns reference real table columns
            for (AssetTableModel atm : assetModel.getTables()) {
                if (atm.isIsRoot() != null && atm.isIsRoot()) {
                    thereIsARootTable = true;
                }
            }
        }
        if (!thereIsARootTable) {
            throw new ValidationException("there is no root table in the asset specification");
        }
        // there has to be a root asset table
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
