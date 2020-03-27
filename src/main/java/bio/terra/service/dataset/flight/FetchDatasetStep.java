package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class FetchDatasetStep implements Step {

    private DatasetDao datasetDao;

    private static Logger logger = LoggerFactory.getLogger(FetchDatasetStep.class);

    public FetchDatasetStep(DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputMap = context.getInputParameters();
        FlightMap workingMap = context.getWorkingMap();

        String datasetId = inputMap.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        Dataset dataset = datasetDao.retrieve(UUID.fromString(datasetId));

        workingMap.put(DatasetWorkingMapKeys.DATASET_NAME, dataset.getName());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // nothing to do, we'll just fail the flight
        return StepResult.getStepResultSuccess();
    }
}

