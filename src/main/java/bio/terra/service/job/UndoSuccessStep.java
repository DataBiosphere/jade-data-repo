package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/** A interface of Step that provides a default implementation of undo that returns success. */
public interface UndoSuccessStep extends Step {
  @Override
  default StepResult undoStep(FlightContext flightContext) {
    return StepResult.getStepResultSuccess();
  }
}
