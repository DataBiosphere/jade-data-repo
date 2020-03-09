package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

import java.util.UUID;

public class CreateDatasetMetadataStep implements Step {

    private DatasetDao datasetDao;
    private DatasetRequestModel datasetRequest;

    private static Logger logger = LoggerFactory.getLogger(CreateDatasetMetadataStep.class);

    public CreateDatasetMetadataStep(DatasetDao datasetDao, DatasetRequestModel datasetRequest) {
        this.datasetDao = datasetDao;
        this.datasetRequest = datasetRequest;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Dataset newDataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
            UUID datasetId = datasetDao.create(newDataset);
            FlightMap workingMap = context.getWorkingMap();
            workingMap.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);

            DatasetSummaryModel datasetSummary =
                DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(newDataset.getDatasetSummary());
            workingMap.put(JobMapKeys.RESPONSE.getKeyName(), datasetSummary);
            return StepResult.getStepResultSuccess();
        } catch (DuplicateKeyException duplicateKeyEx) {
            // dataset creation failed because of a PK violation
            // this happens when trying to create a dataset with the same name as one that already exists
            // in this case, we don't want to delete the metadata in the undo step
            // so, set the DATASET_EXISTS key in the context map to true, to pass this information to the undo step
            context.getWorkingMap().put(DatasetWorkingMapKeys.DATASET_EXISTS, Boolean.TRUE);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, duplicateKeyEx);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // if this step failed because there is already a dataset with this name, then don't delete the metadata
        Boolean datasetExists = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_EXISTS, Boolean.class);
        if (datasetExists != null && datasetExists.booleanValue()) {
            logger.debug("Dataset creation failed because of a PK violation. Not deleting metadata.");
        } else {
            logger.debug("Dataset creation failed for a reason other than a PK violation. Deleting metadata.");
            String datasetName = datasetRequest.getName();
            datasetDao.deleteByName(datasetName);
        }
        return StepResult.getStepResultSuccess();
    }
}

