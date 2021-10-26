package bio.terra.service.snapshot.flight;

import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockSnapshotStep extends OptionalStep {

  private SnapshotDao snapshotDao;
  private UUID snapshotId;

  private static Logger logger = LoggerFactory.getLogger(UnlockSnapshotStep.class);

  public UnlockSnapshotStep(SnapshotDao snapshotDao, UUID snapshotId) {
    this(snapshotDao, snapshotId, OptionalStep::alwaysDo);
  }

  public UnlockSnapshotStep(
      SnapshotDao snapshotDao, UUID snapshotId, Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.snapshotDao = snapshotDao;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
    // In the create case, we won't have the snapshot id at step creation. We'll expect it to be in
    // the working map.
    if (snapshotId == null) {
      snapshotId = context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
      if (snapshotId == null) {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new SnapshotLockException("Expected snapshot id in working map."));
      }
    }
    boolean rowUpdated = snapshotDao.unlock(snapshotId, context.getFlightId());
    logger.debug("rowUpdated on unlock = " + rowUpdated);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
