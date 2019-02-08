package bio.terra;

import bio.terra.model.JobModel;
import bio.terra.model.JobModel.StatusEnum;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;

@Component
public class JobService {

    public JobModel mapFlightStateToJobModel(FlightState flightState) {
        FlightMap inputParameters = flightState.getInputParameters();
        String description = inputParameters.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class);
        StatusEnum status = inputParameters.get(JobMapKeys.STATUS_CODE.getKeyName(), StatusEnum.class);
        String submittedDate = new SimpleDateFormat().format(flightState.getSubmitted());
        String completedDate = new SimpleDateFormat().format(flightState.getCompleted().orElse(null)); // TODO this doesn't seem right?

        JobModel jobModel = new JobModel()
                .id(flightState.getFlightId())
                .description(description)
                .status(status)
                .submitted(submittedDate)
                .completed(completedDate);

        return jobModel;

    }
}
