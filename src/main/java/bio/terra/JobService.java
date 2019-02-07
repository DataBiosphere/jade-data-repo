package bio.terra;

import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.StatusEnum;

import java.text.SimpleDateFormat;


public class JobService {

    public JobModel mapFlightStateToJobModel(FlightState flightState) {
        FlightMap inputParameters = flightState.getInputParameters();
        String description = inputParameters.get(JobMapKeys.DESCRIPTION.toString(), String.class);
        StatusEnum status = inputParameters.get(JobMapKeys.STATUS_CODE.toString(), StatusEnum.class);
        String submittedDate = new SimpleDateFormat().format(flightState.getSubmitted());

        String completedDate = null;
        if (flightState.getCompleted().isPresent()) {
            completedDate = new SimpleDateFormat().format(flightState.getCompleted());
        }

        JobModel jobModel = new JobModel()
                .id(flightState.getFlightId())
                .description(description)
                .status(status)
                .submitted(submittedDate)
                .completed(completedDate);

        return jobModel;

    }
}
