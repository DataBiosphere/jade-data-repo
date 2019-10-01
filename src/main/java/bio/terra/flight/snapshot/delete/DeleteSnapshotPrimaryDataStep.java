package bio.terra.flight.snapshot.delete;

import bio.terra.exception.NotFoundException;
import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.SnapshotSource;
import bio.terra.tabulardata.google.BigQueryPdao;
import bio.terra.dataset.DatasetService;
import bio.terra.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.UUID;

public class DeleteSnapshotPrimaryDataStep implements Step {


    private BigQueryPdao bigQueryPdao;
    private SnapshotService snapshotService;
    private FireStoreDependencyDao dependencyDao;
    private FireStoreDao fileDao;
    private UUID snapshotId;
    private DatasetService datasetService;

    public DeleteSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        SnapshotService snapshotService,
                                        FireStoreDependencyDao dependencyDao,
                                        FireStoreDao fileDao,
                                        UUID snapshotId,
                                        DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotService = snapshotService;
        this.dependencyDao = dependencyDao;
        this.fileDao = fileDao;
        this.snapshotId = snapshotId;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Snapshot snapshot = snapshotService.retrieveSnapshot(snapshotId);
            bigQueryPdao.deleteSnapshot(snapshot);

            // Remove snapshot file references from the underlying datasets
            for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
                Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
                dependencyDao.deleteSnapshotFileDependencies(
                    dataset,
                    snapshotId.toString());
            }
            fileDao.deleteFilesFromSnapshot(snapshot);
        } catch (NotFoundException nfe) {
            // If we do not find the dataset, we assume things are already clean
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // This step is not undoable. We only get here when the
        // metadata delete that comes after will has a dismal failure.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalStateException("Attempt to undo permanent delete"));
    }
}

