package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LockDatasetStep implements Step {

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    private final DatasetDao datasetDao;
    private final UUID datasetId;

    public LockDatasetStep(DatasetDao datasetDao, UUID datasetId) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            datasetDao.lock(datasetId, context.getFlightId());

            return StepResult.getStepResultSuccess();
        } catch (DatasetLockException ex) {
            logger.debug("Issue locking this Dataset", ex);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        boolean rowUpdated = datasetDao.unlock(datasetId, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }
}

