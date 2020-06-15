package bio.terra.service.dataset.flight;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.common.exception.RetryQueryException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.UUID;

public class LockDatasetStep implements Step {

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    private final DatasetDao datasetDao;
    private final ConfigurationService configService;
    private final UUID datasetId;
    private final boolean sharedLock; // default to false
    private final boolean suppressNotFoundException; // default to false

    public LockDatasetStep(DatasetDao datasetDao,
                           ConfigurationService configService,
                           UUID datasetId,
                           boolean sharedLock) {
        this(datasetDao, configService, datasetId, sharedLock, false);
    }

    public LockDatasetStep(DatasetDao datasetDao, ConfigurationService configService,
                           UUID datasetId, boolean sharedLock, boolean suppressNotFoundException) {
        this.datasetDao = datasetDao;
        this.configService = configService;
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
        DataAccessException faultInsert = getFaultToInsert();
        try {
            if (sharedLock) {
                try {
                    datasetDao.lockShared(datasetId, context.getFlightId(), faultInsert);
                } catch (RetryQueryException retryQueryException) {
                    return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
                }
            } else {
                datasetDao.lockExclusive(datasetId, context.getFlightId());
            }

            return StepResult.getStepResultSuccess();
        } catch (DatasetLockException ex) {
            logger.debug("Issue locking this Dataset", ex);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
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

    private DataAccessException getFaultToInsert() {
        if (configService.testInsertFault(ConfigEnum.FILE_INGEST_SHARED_LOCK_RETRY_FAULT)) {
            logger.info("LockDatasetStep - insert RETRY fault to throw during lockShared()");
            return new OptimisticLockingFailureException(
                "TEST RETRY SHARED LOCK - RETRIABLE EXCEPTION - insert fault, throwing shared lock exception");
        } else if (configService.testInsertFault(ConfigEnum.FILE_INGEST_SHARED_LOCK_FATAL_FAULT)) {
            logger.info("LockDatasetStep - insert FATAL fault to throw during lockShared()");
            return new DataIntegrityViolationException(
                "TEST RETRY SHARED LOCK - FATAL EXCEPTION - insert fault, throwing shared lock exception");
        }
        return null;
    }
}

