package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.common.exception.RetryQueryException;
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
    private final boolean sharedLock; // default to false
    private final boolean suppressNotFoundException; // default to false

    public LockDatasetStep(DatasetDao datasetDao,
                           UUID datasetId,
                           boolean sharedLock) {
        this(datasetDao, datasetId, sharedLock, false);
    }

    public LockDatasetStep(DatasetDao datasetDao,
                           UUID datasetId, boolean sharedLock, boolean suppressNotFoundException) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;

        // this will be set to true for a shared lock, false for an exclusive lock
        this.sharedLock = sharedLock;

        // this will be set to true in cases where we don't want to fail if the dataset metadata record doesn't exist.
        // for example, dataset deletion. we want multiple deletes to succeed, not throw a lock or notfound exception.
        // for most cases, this should be set to false because we expect the dataset metadata record to exist.
        this.suppressNotFoundException = suppressNotFoundException;
    }

    @Override
    public StepResult doStep(FlightContext context) {

        try {
            if (sharedLock) {
                logger.info("Attempt to acquire shared lock");
                datasetDao.lockShared(datasetId, context.getFlightId());
            } else {
                logger.info("Attempt to acquire exclusive lock");
                datasetDao.lockExclusive(datasetId, context.getFlightId());
            }
            return StepResult.getStepResultSuccess();
        } catch (DatasetNotFoundException notFoundEx) {
            if (suppressNotFoundException) {
                logger.debug("Suppressing DatasetNotFoundException");
                return new StepResult(StepStatus.STEP_RESULT_SUCCESS);
            } else {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, notFoundEx);
            }
        } catch (RetryQueryException retryQueryException) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        boolean rowUpdated;
        if (sharedLock) {
            logger.info("UNDO: Attempt to unlock shared lock");
            rowUpdated = datasetDao.unlockShared(datasetId, context.getFlightId());
        } else {
            logger.info("UNDO: Attempt to unlock exclusive lock");
            rowUpdated = datasetDao.unlockExclusive(datasetId, context.getFlightId());
        }
        logger.info("UNDO: rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }
}

