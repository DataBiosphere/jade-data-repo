package bio.terra.service.snapshot.flight;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record UnlockSnapshotStep(SnapshotDao snapshotDao, UUID snapshotId)
    implements DefaultUndoStep {

  private static final Logger logger = LoggerFactory.getLogger(UnlockSnapshotStep.class);

  @Override
  public StepResult doStep(FlightContext context) {
    // In the create case, we won't have the snapshot id at step creation. We'll expect it to be in
    // the working map.
    UUID id = snapshotId;
    if (id == null) {
      if (!context.getWorkingMap().containsKey(SnapshotWorkingMapKeys.SNAPSHOT_ID)) {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new SnapshotLockException("Expected snapshot id in working map."));
      }
      id = context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    }
    boolean rowUpdated = snapshotDao.unlock(id, context.getFlightId());
    logger.debug("rowUpdated on unlock = " + rowUpdated);

    return StepResult.getStepResultSuccess();
  }
}
