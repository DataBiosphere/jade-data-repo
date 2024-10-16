package bio.terra.service.load.flight;

import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class LoadLockStep implements Step {
  private final LoadService loadService;

  /**
   * This step is meant to be shared by dataset and filesystem flights for locking the load tag.
   *
   * <p>It expects the following to be available in the flight context:
   *
   * <ul>
   *   <li>{@link LoadMapKeys#LOAD_TAG} in the input parameters or working map
   *   <li>{@link JobMapKeys#DATASET_ID} in the input parameters
   * </ul>
   */
  public LoadLockStep(LoadService loadService) {
    this.loadService = loadService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    LoadLockKey loadLockKey = loadService.getLoadLockKey(context);
    try {
      UUID loadId = loadService.lockLoad(loadLockKey, context.getFlightId());
      FlightMap workingMap = context.getWorkingMap();
      workingMap.put(LoadMapKeys.LOAD_ID, loadId.toString());
      return StepResult.getStepResultSuccess();
    } catch (LoadLockedException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    LoadLockKey loadLockKey = loadService.getLoadLockKey(context);
    loadService.unlockLoad(loadLockKey, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
