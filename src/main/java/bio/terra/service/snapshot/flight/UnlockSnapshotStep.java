package bio.terra.service.snapshot.flight;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnlockSnapshotStep implements Step {

    private SnapshotDao snapshotDao;
    private String snapshotName;

    private static Logger logger = LoggerFactory.getLogger(UnlockSnapshotStep.class);

    public UnlockSnapshotStep(SnapshotDao snapshotDao, String snapshotName) {
        this.snapshotDao = snapshotDao;
        this.snapshotName = snapshotName;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean rowUpdated = snapshotDao.unlock(snapshotName, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

