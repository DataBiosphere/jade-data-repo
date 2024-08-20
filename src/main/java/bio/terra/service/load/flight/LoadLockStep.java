package bio.terra.service.load.flight;

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
  private final UUID datasetId;

  /**
   * This step is meant to be shared by dataset and filesystem flights for locking the load tag. It
   * expects to find LoadMapKeys.LOAD_TAG in the parameters or working map.
   */
  public LoadLockStep(LoadService loadService, UUID datasetId) {
    this.loadService = loadService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    String loadTag = loadService.getLoadTag(context);
    try {
      UUID loadId = loadService.lockLoad(loadTag, context.getFlightId(), datasetId);
      FlightMap workingMap = context.getWorkingMap();
      workingMap.put(LoadMapKeys.LOAD_ID, loadId.toString());
      return StepResult.getStepResultSuccess();
    } catch (LoadLockedException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    String loadTag = loadService.getLoadTag(context);
    loadService.unlockLoad(loadTag, context.getFlightId(), datasetId);
    return StepResult.getStepResultSuccess();
  }
}
