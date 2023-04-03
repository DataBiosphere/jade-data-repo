package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/** A subclass of Step that provides a default implementation of undo that returns success. */
public interface DefaultUndoStep extends Step {
  @Override
  default StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
