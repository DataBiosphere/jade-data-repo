package bio.terra.flight.dataset.delete;

import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.metadata.SnapshotSummary;
import bio.terra.metadata.Dataset;
import bio.terra.service.JobMapKeys;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;
import java.util.UUID;

public class DeleteDatasetValidateStep implements Step {
    private SnapshotDao snapshotDao;
    private FireStoreDependencyDao dependencyDao;
    private DatasetService datasetService;
    public DeleteDatasetValidateStep(SnapshotDao snapshotDao,
                                   FireStoreDependencyDao dependencyDao,
                                   DatasetService datasetService) {
        this.snapshotDao = snapshotDao;
        this.dependencyDao = dependencyDao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID datasetId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        List<SnapshotSummary> snapshots = snapshotDao.retrieveSnapshotsForDataset(datasetId);
        Dataset dataset = datasetService.retrieve(datasetId);

        if (snapshots.size() != 0) {
            throw new ValidationException("Can not delete a dataset being used by snapshots");
        }
        // Sanity check - validate that there are no stray file references. There should be none left
        // if there are no snapshots returned from retrieveSnapshotsForDataset.
        if (dependencyDao.datasetHasSnapshotReference(dataset)) {
            throw new FileSystemCorruptException("File system has snapshot dependencies; metadata does not");
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // no undo is possible
        return StepResult.getStepResultSuccess();
    }

}
