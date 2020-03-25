package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LockDatasetStep implements Step {

    private DatasetDao datasetDao;
    private String datasetName;

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    public LockDatasetStep(DatasetDao datasetDao, String datasetName) {
        this.datasetDao = datasetDao;
        this.datasetName = datasetName;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            FlightMap workingMap = context.getWorkingMap();
            workingMap.put(DatasetWorkingMapKeys.DATASET_NAME, datasetName);

            datasetDao.lock(datasetName, context.getFlightId());

            return StepResult.getStepResultSuccess();
        } catch (DatasetLockException lockedEx) {
            logger.debug("Another flight has already locked this Dataset", lockedEx);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, lockedEx);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // try to unlock the flight if something went wrong above
        // note the unlock will only clear the flightid if it's set to this flightid
        boolean rowUpdated = datasetDao.unlock(datasetName, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }
}

