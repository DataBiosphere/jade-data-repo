package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
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
  private final AuthenticatedUserRequest userReq;

  public GetSnapshotIdsStep(
      SnapshotDao snapshotDao, UUID datasetId, AuthenticatedUserRequest userReq) {
    this.snapshotDao = snapshotDao;
    this.datasetId = datasetId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<UUID> snapshotIds = snapshotDao.getSnapshotIds(datasetId);
    context.getWorkingMap().put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    return StepResult.getStepResultSuccess();
  }
}
