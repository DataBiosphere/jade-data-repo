package bio.terra.service;

import bio.terra.flight.FlightResponse;
import bio.terra.flight.FlightUtils;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.exception.InvalidResultStateException;
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
            // When the flight is completed, we want to put the status code from the flight
            // into the job model.
            FlightMap resultMap = getResultMap(flightState);
            statusCode = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
            if (statusCode == null) {
                // Error: this is a flight coding bug where the status code was not filled in properly.
                throw new InvalidResultStateException("No status code returned from flight");
            }

            completedDate = modelDateFormat.format(flightState.getCompleted().get());
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
        HttpStatus responseStatus;
        String locationHeader;

        if (flightState.getCompleted().isPresent()) {
            responseStatus = HttpStatus.OK;
            locationHeader = String.format("/api/repository/v1/jobs/%s/result", jobId);
        } else {
            responseStatus = HttpStatus.ACCEPTED;
            locationHeader = String.format("/api/repository/v1/jobs/%s", jobId);
        }

        return ResponseEntity
                .status(responseStatus)
                .header("Location", locationHeader)
                .body(jobModel);
    }

    public ResponseEntity<Object> retrieveJobResult(String jobId) {
        FlightState flightState = stairway.getFlightState(jobId);
        FlightResponse flightResponse = FlightUtils.makeFlightResponse(flightState);

        // If the flight isn't done, we call that a bad request.
        if (!flightResponse.isFlightComplete()) {
            ErrorModel errorRunning = new ErrorModel()
                    .message("Attempt to retrieve job result before job is complete; job id: " + jobId);
            flightResponse.response(errorRunning).statusCode(HttpStatus.BAD_REQUEST);
        }

        return new ResponseEntity<>(flightResponse.getResponse(), flightResponse.getStatusCode());
    }

    private FlightMap getResultMap(FlightState flightState) {
        FlightMap resultMap = flightState.getResultMap().orElse(null);
        if (resultMap == null) {
            throw new InvalidResultStateException("No result map returned from flight");
        }
        return resultMap;
    }

}
