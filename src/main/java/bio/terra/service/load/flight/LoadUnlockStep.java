package bio.terra.service.load.flight;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;

public class LoadUnlockStep extends DefaultUndoStep {
  private final LoadService loadService;

  /**
   * This step is meant to be shared by dataset and filesystem flights for removing this flight's
   * lock on the load tag in the targeted dataset.
   *
   * <p>It expects the following to be available in the flight context:
   *
   * <ul>
   *   <li>{@link LoadMapKeys#LOAD_TAG} in the input parameters or working map
   *   <li>{@link JobMapKeys#DATASET_ID} in the input parameters
   * </ul>
   */
  public LoadUnlockStep(LoadService loadService) {
    this.loadService = loadService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    LoadLockKey loadLockKey = loadService.getLoadLockKey(context);
    loadService.unlockLoad(loadLockKey, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
