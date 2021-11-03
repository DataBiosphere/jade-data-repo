package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/**
 * A subclass of Step that provides a default implementation of undo.
 */
public abstract class DefaultUndoStep implements Step {
  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
