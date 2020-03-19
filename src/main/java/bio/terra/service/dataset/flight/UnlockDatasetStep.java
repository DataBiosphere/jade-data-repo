package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnlockDatasetStep implements Step {

    private DatasetDao datasetDao;

    private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

    public UnlockDatasetStep(DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String datasetName = workingMap.get(DatasetWorkingMapKeys.DATASET_NAME, String.class);

        boolean rowUpdated = datasetDao.unlock(datasetName, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

