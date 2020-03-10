package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.job.LockBehaviorFlags;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class LockDatasetStep implements Step {

    private DatasetDao datasetDao;
    private DatasetRequestModel datasetRequest;
    private LockBehaviorFlags lockFlag;

    private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

    public LockDatasetStep(DatasetDao datasetDao, DatasetRequestModel datasetRequest, LockBehaviorFlags lockFlag) {
        this.datasetDao = datasetDao;
        this.datasetRequest = datasetRequest;
        this.lockFlag = lockFlag;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            String datasetName = datasetRequest.getName();
            UUID defaultProfileId = UUID.fromString(datasetRequest.getDefaultProfileId());
            datasetDao.lock(datasetName, defaultProfileId, context.getFlightId(), lockFlag);

            FlightMap workingMap = context.getWorkingMap();
            workingMap.put(DatasetWorkingMapKeys.DATASET_NAME, datasetName);

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
        try {
            boolean rowUpdated = datasetDao.unlock(datasetRequest.getName(), context.getFlightId());
            logger.debug("rowUpdated on unlock = " + rowUpdated);
        } catch (Exception ex) {
            logger.debug("Dataset unlock threw an exception", ex);
        }

        return StepResult.getStepResultSuccess();
    }
}

