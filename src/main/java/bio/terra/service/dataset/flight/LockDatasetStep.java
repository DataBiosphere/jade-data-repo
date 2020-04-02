package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidLockUsageException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LockDatasetStep extends LockDatasetBaseStep {

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    public LockDatasetStep(DatasetDao datasetDao, String datasetName) {
        super(datasetDao, datasetName);
    }

    public LockDatasetStep(DatasetDao datasetDao) {
        super(datasetDao);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            getDatasetDao().lock(getDatasetName(context), context.getFlightId());

            return StepResult.getStepResultSuccess();
        } catch (DatasetLockException | InvalidLockUsageException | DatasetNotFoundException ex) {
            logger.debug("Issue locking this Dataset", ex);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        try {
            boolean rowUpdated = getDatasetDao().unlock(getDatasetName(context), context.getFlightId());
            logger.debug("rowUpdated on unlock = " + rowUpdated);
        } catch (InvalidLockUsageException | DatasetNotFoundException ex) {
            logger.debug("Issue unlocking this Dataset", ex);
        }

        return StepResult.getStepResultSuccess();
    }
}

