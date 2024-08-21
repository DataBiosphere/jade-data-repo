package bio.terra.service.load.flight;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class LoadUnlockStep extends DefaultUndoStep {
  private final LoadService loadService;

  /**
   * This step is meant to be shared by dataset and filesystem flights for unlocking the load tag.
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
    String loadTag = loadService.getLoadTag(context);
    UUID datasetId = IngestUtils.getDatasetId(context);
    loadService.unlockLoad(loadTag, context.getFlightId(), datasetId);
    return StepResult.getStepResultSuccess();
  }
}
