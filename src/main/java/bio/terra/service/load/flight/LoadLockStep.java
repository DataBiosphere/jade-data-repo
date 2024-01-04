package bio.terra.service.load.flight;

import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

// This step is meant to be shared by dataset and filesystem flights for locking the load tag.
// It expects to find LoadMapKeys.LOAD_TAG in the parameters or working map.

public class LoadLockStep implements Step {
  private final LoadService loadService;

  public LoadLockStep(LoadService loadService) {
    this.loadService = loadService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    String loadTag = loadService.getLoadTag(context);
    UUID loadId = loadService.lockLoad(loadTag, context.getFlightId());
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(LoadMapKeys.LOAD_ID, loadId.toString());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    String loadTag = loadService.getLoadTag(context);
    loadService.unlockLoad(loadTag, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
