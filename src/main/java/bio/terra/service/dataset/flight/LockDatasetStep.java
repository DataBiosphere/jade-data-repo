package bio.terra.service.dataset.flight;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LockDatasetStep implements Step {

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    private final DatasetDao datasetDao;
    private final UUID datasetId;
    private final boolean sharedLock; // default to false
    private final boolean suppressNotFoundException; // default to false
    private ConfigurationService configurationService;

    public LockDatasetStep(DatasetDao datasetDao, UUID datasetId, boolean sharedLock,
                           ConfigurationService configurationService) {
        this(datasetDao, datasetId, sharedLock, false, configurationService);
    }

    public LockDatasetStep(DatasetDao datasetDao, UUID datasetId,
                           boolean sharedLock, boolean suppressNotFoundException,
                           ConfigurationService configurationService) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;

        // this will be set to true for a shared lock, false for an exclusive lock
        this.sharedLock = sharedLock;

        // this will be set to true in cases where we don't want to fail if the dataset metadata record doesn't exist.
        // for example, dataset deletion. we want multiple deletes to succeed, not throw a lock or notfound exception.
        // for most cases, this should be set to false because we expect the dataset metadata record to exist.
        this.suppressNotFoundException = suppressNotFoundException;

        this.configurationService = configurationService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        try {
            if (sharedLock) {
                datasetDao.lockShared(datasetId, context.getFlightId());
            } else {
                datasetDao.lockExclusive(datasetId, context.getFlightId());
                logger.info("MICHAEL" + datasetDao.getExclusiveLock(datasetId));
            }

            if (configurationService.testInsertFault(ConfigEnum.LOCK_DATASET_STOP_FAULT)) {
                logger.info("LOCK_DATASET_STOP_FAULT");
                while (!configurationService.testInsertFault(ConfigEnum.LOCK_DATASET_CONTINUE_FAULT)) {
                    logger.info("Sleeping for CONTINUE FAULT");
                    TimeUnit.SECONDS.sleep(5);
                }
                logger.info("LOCK_DATASET_CONTINUE_FAULT");
            }

            return StepResult.getStepResultSuccess();
        } catch (DatasetLockException ex) {
            logger.debug("Issue locking this Dataset", ex);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
        } catch (DatasetNotFoundException notFoundEx) {
            if (suppressNotFoundException) {
                logger.debug("Suppressing DatasetNotFoundException");
                return new StepResult(StepStatus.STEP_RESULT_SUCCESS);
            } else {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, notFoundEx);
            }
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        boolean rowUpdated;
        if (sharedLock) {
            rowUpdated = datasetDao.unlockShared(datasetId, context.getFlightId());
        } else {
            rowUpdated = datasetDao.unlockExclusive(datasetId, context.getFlightId());
        }
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }
}

