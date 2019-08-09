package bio.terra.flight.dataset.create;

import bio.terra.metadata.Dataset;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.service.JobMapKeys;
import bio.terra.service.DatasetService;
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
        UUID datasetId = workingMap.get("datasetId", UUID.class);
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

