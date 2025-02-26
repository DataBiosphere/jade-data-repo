package bio.terra.service.dataset.flight.inheritSteward;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class InheritStewardSetFlagStep implements Step {
  private final DatasetDao datasetDao;
  private final UUID datasetId;
  private final boolean enableInheritSteward;

  public InheritStewardSetFlagStep(
      DatasetDao datasetDao, UUID datasetId, boolean enableInheritSteward) {
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
    this.enableInheritSteward = enableInheritSteward;
  }

  /**
   * Set the inherit steward flag on the dataset.
   *
   * @param context flight context
   * @return step result
   */
  @Override
  public StepResult doStep(FlightContext context) {
    boolean patchSucceeded = datasetDao.setInheritSteward(datasetId, enableInheritSteward);
    if (!patchSucceeded) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Unable to update inherit steward flag"));
    }
    return StepResult.getStepResultSuccess();
  }

  /**
   * Undo the flag until we have a successful run of entire flight. Since the flight only runs if
   * the flag is being changed, set the flag to the opposite.
   *
   * @param context flight context
   * @return step result
   */
  @Override
  public StepResult undoStep(FlightContext context) {
    boolean patchSucceeded = datasetDao.setInheritSteward(datasetId, !enableInheritSteward);
    if (!patchSucceeded) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Unable to update inherit steward flag"));
    }
    return StepResult.getStepResultSuccess();
  }
}
