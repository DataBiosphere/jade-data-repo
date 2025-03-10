package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class GetSnapshotIdsStep extends DefaultUndoStep {
  private final SnapshotDao snapshotDao;
  private final UUID datasetId;

  public GetSnapshotIdsStep(SnapshotDao snapshotDao, UUID datasetId) {
    this.snapshotDao = snapshotDao;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<UUID> snapshotIds = snapshotDao.getSnapshotIds(datasetId);
    context.getWorkingMap().put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    return StepResult.getStepResultSuccess();
  }
}
