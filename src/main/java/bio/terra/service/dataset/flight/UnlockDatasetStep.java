package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnlockDatasetStep extends LockDatasetBaseStep {

    private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

    public UnlockDatasetStep(DatasetDao datasetDao, String datasetName) {
        super(datasetDao, datasetName);
    }

    public UnlockDatasetStep(DatasetDao datasetDao) {
        super(datasetDao);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean rowUpdated = getDatasetDao().unlock(getDatasetName(), context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

