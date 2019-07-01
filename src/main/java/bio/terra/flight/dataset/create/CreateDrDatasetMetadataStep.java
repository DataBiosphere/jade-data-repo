package bio.terra.flight.dataset.create;

import bio.terra.dao.DrDatasetDao;
import bio.terra.metadata.DrDataset;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateDrDatasetMetadataStep implements Step {

    private DrDatasetDao datasetDao;

    public CreateDrDatasetMetadataStep(DrDatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        DrDatasetRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
            DrDatasetRequestModel.class);
        DrDataset newDataset = DrDatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        UUID datasetId = datasetDao.create(newDataset);
        workingMap.put("datasetId", datasetId);
        DrDatasetSummaryModel datasetSummary =
            DrDatasetJsonConversion.datasetSummaryModelFromDatasetSummary(newDataset);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), datasetSummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DrDatasetRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
            DrDatasetRequestModel.class);
        String datasetName = datasetRequest.getName();
        datasetDao.deleteByName(datasetName);
        return StepResult.getStepResultSuccess();
    }
}

