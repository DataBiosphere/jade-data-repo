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
    private String datasetName;

    private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

    public UnlockDatasetStep(DatasetDao datasetDao, String datasetName) {
        this.datasetDao = datasetDao;
        this.datasetName = datasetName;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean rowUpdated = datasetDao.unlock(datasetName, context.getFlightId());
        logger.debug("rowUpdated on unlock = " + rowUpdated);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

