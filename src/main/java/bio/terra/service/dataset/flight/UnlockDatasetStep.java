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


public class UnlockDatasetStep implements Step {

    private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

    private final DatasetDao datasetDao;
    private UUID datasetId;

    public UnlockDatasetStep(DatasetDao datasetDao, UUID datasetId) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    public UnlockDatasetStep(DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // In the create case, we won't have the dataset id at step creation. We'll expect it to be in the working map.
        if (datasetId == null) {
            datasetId = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
            if (datasetId == null) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                    new DatasetLockException("Expected dataset id in working map."));
            }
        }

        boolean rowUpdated = datasetDao.unlock(datasetId, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

