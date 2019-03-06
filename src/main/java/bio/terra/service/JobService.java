package bio.terra.service;

import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class JobService {

    private final Stairway stairway;
    private final FastDateFormat modelDateFormat;

    @Autowired
    public JobService(
            Stairway stairway,
            FastDateFormat modelDateFormat
    ) {
        this.stairway = stairway;
        this.modelDateFormat = modelDateFormat;
    }

    public JobModel mapFlightStateToJobModel(FlightState flightState) {
        FlightMap inputParameters = flightState.getInputParameters();
        String description = inputParameters.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class);
        FlightStatus flightStatus = flightState.getFlightStatus();
        String submittedDate = modelDateFormat.format(flightState.getSubmitted());
        JobModel.JobStatusEnum jobStatus = getJobStatus(flightStatus);

        String completedDate = null;
        HttpStatus statusCode = HttpStatus.ACCEPTED;

        if (flightState.getCompleted().isPresent()) {
            completedDate = modelDateFormat.format(flightState.getCompleted().get());
            statusCode = HttpStatus.OK;
        }

        JobModel jobModel = new JobModel()
                .id(flightState.getFlightId())
                .description(description)
                .jobStatus(jobStatus)
                .statusCode(statusCode.value())
                .submitted(submittedDate)
                .completed(completedDate);

        return jobModel;
    }

    private JobModel.JobStatusEnum getJobStatus(FlightStatus flightStatus) {
        switch (flightStatus) {
            case ERROR:
                return JobModel.JobStatusEnum.FAILED;
            case FATAL:
                return JobModel.JobStatusEnum.FAILED;
            case RUNNING:
                return JobModel.JobStatusEnum.RUNNING;
            case SUCCESS:
                return JobModel.JobStatusEnum.SUCCEEDED;
        }
        return JobModel.JobStatusEnum.FAILED;
    }

    public ResponseEntity<List<JobModel>> enumerateJobs(int offset, int limit) {
        List<FlightState> flightStateList = stairway.getFlights(offset, limit);
        List<JobModel> jobModelList = new ArrayList<>();
        for (FlightState flightState : flightStateList) {
            JobModel jobModel = mapFlightStateToJobModel(flightState);
            jobModelList.add(jobModel);
        }
        return new ResponseEntity<>(jobModelList, HttpStatus.OK);
    }

    public ResponseEntity<JobModel> retrieveJob(String jobId) {
        FlightState flightState = stairway.getFlightState(jobId);
        JobModel jobModel = mapFlightStateToJobModel(flightState);
        HttpStatus status;
        String locationHeader;

        if (flightState.getCompleted().isPresent()) {
            status = HttpStatus.OK;
            locationHeader = String.format("/api/repository/v1/jobs/%s/result", jobId);

        } else {
            status = HttpStatus.ACCEPTED;
            locationHeader = String.format("/api/repository/v1/jobs/%s", jobId);
        }
        ResponseEntity responseEntity = ResponseEntity
                .status(status)
                .header("Location", locationHeader)
                .body(jobModel);

        return responseEntity;
    }

    public ResponseEntity<Object> retrieveJobResult(String jobId) {
        FlightState flightState = stairway.getFlightState(jobId);

        // If the flight isn't done, we call that a bad request.
        if (!flightState.getCompleted().isPresent()) {
            ErrorModel errorRunning = new ErrorModel()
                    .message("Attempt to retrieve job result before job is complete; job id: " + jobId);
            HttpStatus statusRunning = HttpStatus.BAD_REQUEST;
            return new ResponseEntity<>(errorRunning, statusRunning);
        }

        FlightMap resultMap = flightState.getResultMap().get();
        HttpStatus returnedStatus = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
        Object returnedModel = resultMap.get(JobMapKeys.RESPONSE.getKeyName(), Object.class);

        ResponseEntity<Object> responseEntity;
        switch (flightState.getFlightStatus()) {
            case FATAL:
            case ERROR:
                // If the flight failed without supplying a status code and response, then we generate one
                // from the flight error. This handles the case of thrown errors that the step code does
                // not handle.
                if (returnedStatus == null) {
                    returnedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
                }
                if (returnedModel == null) {
                    String msg = flightState.getErrorMessage().orElse("Job failed with no error message!");
                    ErrorModel errorModel = new ErrorModel().message(msg);
                    responseEntity = new ResponseEntity<>(errorModel, returnedStatus);
                } else {
                    responseEntity = new ResponseEntity<>(returnedModel, returnedStatus);
                }
                return responseEntity;

            case RUNNING:
                // This should never happen
                throw new IllegalStateException("Job marked running but has completion time");

            case SUCCESS:

                if (returnedStatus == null) {
                    // Error: this is a flight coding bug where the status code
                    // was not filled in properly.
                    throw new IllegalStateException("No status code returned from flight");
                }
                if (returnedModel == null) {
                    // Flights do not have to set the return model. We return an empty response body
                    responseEntity = new ResponseEntity<>(returnedStatus);
                } else {
                    responseEntity = new ResponseEntity<>(returnedModel, returnedStatus);
                }
                return responseEntity;

            default:
                throw new IllegalStateException("Switch default should never be taken");
        }
    }

}
