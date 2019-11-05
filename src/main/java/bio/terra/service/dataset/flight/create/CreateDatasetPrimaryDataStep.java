package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.PrimaryDataAccess;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.Optional;
import java.util.UUID;

public class CreateDatasetPrimaryDataStep implements Step {
    private final PrimaryDataAccess pdao;
    private final DatasetDao datasetDao;
    private final DataLocationService dataLocationService;

    public CreateDatasetPrimaryDataStep(
        PrimaryDataAccess pdao, DatasetDao datasetDao, DataLocationService dataLocationService) {
        this.pdao = pdao;
        this.datasetDao = datasetDao;
        this.dataLocationService = dataLocationService;
    }

    private Dataset getDataset(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        return datasetDao.retrieve(datasetId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = getDataset(context);
        dataLocationService.getOrCreateProject(dataset);
        pdao.createDataset(dataset);

        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Dataset dataset = getDataset(context);

        // get the cloud project for the dataset if it exists
        Optional<DatasetDataProject> optDataProject = dataLocationService.getProject(dataset);
        if (optDataProject.isPresent()) {
            // there can only be primary data to delete if a cloud project exists for the dataset
            pdao.deleteDataset(dataset);
        }

        return StepResult.getStepResultSuccess();
    }
}

