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

            if (flightState.getResultMap().isPresent()) {
                statusCode = flightState.getResultMap().get()
                        .get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
            }
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
            locationHeader = String.format("/api/jobs/%s/result", jobId);

        } else {
            status = HttpStatus.ACCEPTED;
            locationHeader  = String.format("/api/jobs/%s", jobId);
        }
        ResponseEntity responseEntity = ResponseEntity
                .status(status)
                .header("Location", locationHeader)
                .body(jobModel);

        return responseEntity;
    }

    public ResponseEntity<Object> retrieveJobResult(String jobId) {
        ResponseEntity responseEntity;
        FlightState flightState = stairway.getFlightState(jobId);
        if (flightState.getCompleted().isPresent()) {
            FlightMap resultMap = flightState.getResultMap().get();
            HttpStatus returnedStatus = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
            if (returnedStatus == null) {
                throw new IllegalStateException("No status code returned from flight");
            }

            Object returnedModel = resultMap.get(JobMapKeys.RESPONSE.getKeyName(), Object.class);
            if (returnedModel == null) {
                responseEntity = new ResponseEntity<>(returnedStatus);
            } else {
                responseEntity = new ResponseEntity<>(returnedModel, returnedStatus);
            }
        } else {
            ErrorModel errorModel = new ErrorModel()
                    .message("Attempt to retrieve job result before job is complete; job id: " + jobId);
            HttpStatus status = HttpStatus.BAD_REQUEST;
            responseEntity = new ResponseEntity<>(errorModel, status);
        }
        return responseEntity;
    }
}
