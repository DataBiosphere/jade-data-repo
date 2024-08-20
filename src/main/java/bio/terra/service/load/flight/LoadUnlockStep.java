package bio.terra.service.load.flight;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class LoadUnlockStep extends DefaultUndoStep {
  private final LoadService loadService;
  private final UUID datasetId;

  /**
   * This step is meant to be shared by dataset and filesystem flights for unlocking the load tag.
   * It expects to find LoadMapKeys.LOAD_TAG in the working map.
   */
  public LoadUnlockStep(LoadService loadService, UUID datasetId) {
    this.loadService = loadService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    String loadTag = loadService.getLoadTag(context);
    loadService.unlockLoad(loadTag, context.getFlightId(), datasetId);
    return StepResult.getStepResultSuccess();
  }
}
