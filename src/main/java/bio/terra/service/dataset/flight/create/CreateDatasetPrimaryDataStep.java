package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.PrimaryDataAccess;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateDatasetPrimaryDataStep implements Step {
    private final PrimaryDataAccess pdao;
    private final DatasetService datasetService;

    public CreateDatasetPrimaryDataStep(PrimaryDataAccess pdao, DatasetService datasetService) {
        this.pdao = pdao;
        this.datasetService = datasetService;
    }

    Dataset getDataset(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        return datasetService.retrieve(datasetId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        pdao.createDataset(getDataset(context));
        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        pdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }
}

