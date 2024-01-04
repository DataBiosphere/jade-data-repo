package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class DeleteSnapshotPrimaryDataGcpStep implements Step {

  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final SnapshotService snapshotService;
  private final FireStoreDao fileDao;
  private final UUID snapshotId;

  public DeleteSnapshotPrimaryDataGcpStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      SnapshotService snapshotService,
      FireStoreDao fileDao,
      UUID snapshotId) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotService = snapshotService;
    this.fileDao = fileDao;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    // Delete Snapshot BigQuery Dataset
    bigQuerySnapshotPdao.deleteSnapshot(snapshot);

    // Delete Snapshot entries from Firestore
    fileDao.deleteFilesFromSnapshot(snapshot);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // This step is not undoable. We only get here when the
    // metadata delete that comes after will has a dismal failure.
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
