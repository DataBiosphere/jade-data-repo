package bio.terra.service.load.flight;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class LoadManualUnlockStep extends DefaultUndoStep {
  private final LoadService loadService;
  private final String lockName;

  /**
   * This step removes any lock held by {@code lockName} in the targeted dataset.
   *
   * <p>It expects {@link JobMapKeys#DATASET_ID} to be available in the flight's input parameters.
   *
   * @param lockName the name of the lock to remove
   */
  public LoadManualUnlockStep(LoadService loadService, String lockName) {
    this.loadService = loadService;
    this.lockName = lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    UUID datasetId = IngestUtils.getDatasetId(context);
    loadService.unlockLoad(new LoadLockKey(datasetId), lockName);
    return StepResult.getStepResultSuccess();
  }
}
