package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class SetInheritStewardFlagStep implements Step {
  private final DatasetDao datasetDao;
  private final UUID datasetId;
  private final boolean enableInheritSteward;

  public SetInheritStewardFlagStep(
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
    datasetDao.setInheritSteward(datasetId, enableInheritSteward);
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
    datasetDao.setInheritSteward(datasetId, !enableInheritSteward);
    return StepResult.getStepResultSuccess();
  }
}
