package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

public class JobServiceTestStep implements Step {
  private String description;

  public JobServiceTestStep(String description) {
    this.description = description;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // Configure the results
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), description);
    context.getWorkingMap().put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
