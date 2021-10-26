package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public abstract class BaseStep implements Step {
  @Override
  public StepResult undoStep(FlightContext context) {
    // This step has no side effects
    return StepResult.getStepResultSuccess();
  }
}
