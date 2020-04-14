package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DeleteSnapshotPrimaryDataStep implements Step {

    private static Logger logger = LoggerFactory.getLogger(DeleteSnapshotPrimaryDataStep.class);

    private BigQueryPdao bigQueryPdao;
    private SnapshotService snapshotService;
    private FireStoreDependencyDao dependencyDao;
    private FireStoreDao fileDao;
    private UUID snapshotId;
    private DatasetService datasetService;
    private ConfigurationService configService;

    public DeleteSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        SnapshotService snapshotService,
                                        FireStoreDependencyDao dependencyDao,
                                        FireStoreDao fileDao,
                                        UUID snapshotId,
                                        DatasetService datasetService,
                                        ConfigurationService configService) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotService = snapshotService;
        this.dependencyDao = dependencyDao;
        this.fileDao = fileDao;
        this.snapshotId = snapshotId;
        this.datasetService = datasetService;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            // this fault is used by the SnapshotConnectedTest > testOverlappingDeletes
            if (configService.testInsertFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
                try {
                    logger.info("SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT");
                    while (!configService.testInsertFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
                        logger.info("Sleeping for CONTINUE FAULT");
                        TimeUnit.SECONDS.sleep(5);
                    }
                    logger.info("SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
                } catch (InterruptedException intEx) {
                    Thread.currentThread().interrupt();
                    throw new SnapshotLockException("Unexpected interrupt during snapshot delete lock fault", intEx);
                }
            }

            Snapshot snapshot = snapshotService.retrieve(snapshotId);
            bigQueryPdao.deleteSnapshot(snapshot);

            // Remove snapshot file references from the underlying datasets
            for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
                Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
                dependencyDao.deleteSnapshotFileDependencies(
                    dataset,
                    snapshotId.toString());
            }
            fileDao.deleteFilesFromSnapshot(snapshot);
        } catch (SnapshotNotFoundException | DatasetNotFoundException nfe) {
            // If we do not find the snapshot or dataset, we assume things are already clean
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

