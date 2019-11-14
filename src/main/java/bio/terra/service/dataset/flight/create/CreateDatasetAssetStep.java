package bio.terra.service.dataset.flight.create;

import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateDatasetAssetStep implements Step {

    private DatasetDao datasetDao;
    private DatasetRequestModel datasetRequest;

    public CreateDatasetAssetStep(DatasetDao datasetDao, DatasetRequestModel datasetRequest) {
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

