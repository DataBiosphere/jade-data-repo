package bio.terra.service.dataset.flight.create;

import bio.terra.common.PdaoConstant;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightUtils;
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

    public static Dataset setUtilityTableNames(Dataset dataset) {
        dataset.getTables().forEach(t -> {
            String rawDataName = FlightUtils.randomizeName(PdaoConstant.RAW_DATA_PREFIX + t.getName());
            String sdListName = FlightUtils.randomizeName(PdaoConstant.SOFT_DELETE_PREFIX + t.getName());
            t.rawTableName(rawDataName);
            t.softDeleteTableName(sdListName);
        });
        return dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset newDataset = setUtilityTableNames(
            DatasetJsonConversion.datasetRequestToDataset(datasetRequest));
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

