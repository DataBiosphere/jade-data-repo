package bio.terra.controller;

import bio.terra.controller.exception.ApiException;
import bio.terra.controller.exception.ValidationException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.model.*;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.FlightStatusEnum;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;

import javax.print.attribute.standard.JobMediaSheets;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
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

    public ResponseEntity<List<JobModel>> enumerateJobs(Integer offset, Integer limit){
        List<JobModel> jobModelList = new ArrayList<JobModel>();
        List<FlightState> flightStateList = stairway.getFlights(offset, limit);
        for (FlightState flightState : flightStateList) {
            Optional<FlightMap> resultMapOp = flightState.getResultMap(); // TODO need to throw an error if not there?
            FlightMap resultMap = resultMapOp.get();
            JobModel jobModel = resultMap.get(FlightStatusEnum.RESPONSE.toString(), JobModel.class);
            // why does this want me to make this a string? Should just the enum be okay
            jobModelList.add(jobModel);
        }
        return new ResponseEntity<>(jobModelList, HttpStatus.OK);
    }

    public ResponseEntity<JobModel> retrieveJob(String jobId){
        FlightState flightState = stairway.getFlightState(jobId);
        FlightMap resultMap = new FlightMap();
        if (flightState.getCompleted().isPresent()) {
            resultMap = flightState.getResultMap().get();
        } else { // If the flight is still going... does this still return a jobModel?
            //FlightMap resultMap = flightState.getFlightStatus();
        }
        JobModel jobModel =  resultMap.get("response", JobModel.class); // is there an enum I'm missing?
        return new ResponseEntity<>(jobModel, HttpStatus.OK);
    }

    public ResponseEntity<Object> retrieveJobResult(String jobId){ // TODO I probably want to use a helper method for these two similar
        FlightState flightState = stairway.getFlightState(jobId);
        FlightMap resultMap = flightState.getResultMap().get(); // TODO need to throw an error if not completed??!?!
        Object returnedModel = resultMap.get("response", Object.class);
        return new ResponseEntity<>(returnedModel, HttpStatus.OK);
    }

    public <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        if (result.getFlightStatus() == FlightStatus.SUCCESS) {
            if (result.getResultMap().isPresent()) {
                FlightMap resultMap = result.getResultMap().get();
                return resultMap.get("response", resultClass);
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
