package bio.terra.service;

import bio.terra.model.JobModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Component
public class JobService {

    private final Stairway stairway;

    @Autowired
    public JobService(
            Stairway stairway
    ) {
        this.stairway = stairway;
    }

    public JobModel mapFlightStateToJobModel(FlightState flightState) {
        FlightMap inputParameters = flightState.getInputParameters();
        String description = inputParameters.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class);
        FlightStatus flightStatus = flightState.getFlightStatus(); // needs to be converted -- create a switch
        String submittedDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(flightState.getSubmitted());
        JobModel.JobStatusEnum jobStatus = getJobStatus(flightStatus);

        String completedDate = null;
        HttpStatus statusCode = HttpStatus.ACCEPTED;

        if (flightState.getCompleted().isPresent()) {
            completedDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                    .format(flightState.getCompleted().get());

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
            status = HttpStatus.SEE_OTHER; // HTTP 303
            locationHeader = String.format("/api/jobs/%s/result", jobId);

        } else {
            status = HttpStatus.OK;
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
            Object returnedModel = resultMap.get(JobMapKeys.RESPONSE.getKeyName(), Object.class);
            HttpStatus returnedStatus = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
            // TODO handle case where cant find key
            responseEntity = new ResponseEntity<>(returnedModel, returnedStatus);
        } else {
            HttpStatus status = HttpStatus.BAD_REQUEST;
            responseEntity = new ResponseEntity<>(status);
        }
        return responseEntity;
    }
}
