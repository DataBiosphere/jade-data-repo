package bio.terra.service.snapshot.flight;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class LockSnapshotStep implements Step {

    private SnapshotDao snapshotDao;
    private UUID snapshotId;

    private static Logger logger = LoggerFactory.getLogger(LockSnapshotStep.class);

    public LockSnapshotStep(SnapshotDao snapshotDao, UUID snapshotId) {
        this.snapshotDao = snapshotDao;
        this.snapshotId = snapshotId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            snapshotDao.lock(snapshotId, context.getFlightId());

            return StepResult.getStepResultSuccess();
        } catch (SnapshotLockException lockedEx) {
            logger.debug("Another flight has already locked this Snapshot", lockedEx);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, lockedEx);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        boolean rowUpdated = snapshotDao.unlock(snapshotId, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }
}

