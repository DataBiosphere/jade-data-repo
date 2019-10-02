package bio.terra.flight.dataset.create;

import bio.terra.dataset.DatasetDao;
import bio.terra.flight.dataset.DatasetWorkingMapKeys;
import bio.terra.dataset.Dataset;
import bio.terra.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateDatasetMetadataStep implements Step {

    private DatasetDao datasetDao;
    private DatasetRequestModel datasetRequest;

    public CreateDatasetMetadataStep(DatasetDao datasetDao, DatasetRequestModel datasetRequest) {
        this.datasetDao = datasetDao;
        this.datasetRequest = datasetRequest;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset newDataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        UUID datasetId = datasetDao.create(newDataset);
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);

        DatasetSummaryModel datasetSummary =
            DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(newDataset.getDatasetSummary());
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), datasetSummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        String datasetName = datasetRequest.getName();
        datasetDao.deleteByName(datasetName);
        return StepResult.getStepResultSuccess();
    }
}

