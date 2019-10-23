package bio.terra.service.dataset.flight.create;

import bio.terra.common.PrimaryDataAccess;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
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
    private final DatasetService datasetService;
    private final DataLocationService dataLocationService;

    public CreateDatasetPrimaryDataStep(PrimaryDataAccess pdao,
                                        DatasetService datasetService,
                                        DataLocationService dataLocationService) {
        this.pdao = pdao;
        this.datasetService = datasetService;
        this.dataLocationService = dataLocationService;
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
        // check if project was created
        UUID datasetId = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        Optional<DatasetDataProject> datasetProject = dataLocationService.getProjectForDatasetId(datasetId);
        if (datasetProject.isPresent()) {
            // if there is no project associated with this dataset, then there won't be primary data to delete,
            // so no need to call the below method.
            // also the below method will try to create a project if one doesn't already exist, and we don't
            // want to do that in the undo direction.
            pdao.deleteDataset(getDataset(context));
        }
        return StepResult.getStepResultSuccess();
    }
}

