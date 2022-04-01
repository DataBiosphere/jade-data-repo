package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryException;
import java.util.UUID;

public class DeleteSnapshotSourceDatasetDataGcpStep implements Step {
  private final FireStoreDependencyDao dependencyDao;
  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final UUID snapshotId;
  private final DatasetService datasetService;
  private final SnapshotService snapshotService;

  public DeleteSnapshotSourceDatasetDataGcpStep(
      FireStoreDependencyDao dependencyDao,
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      UUID snapshotId,
      DatasetService datasetService,
      SnapshotService snapshotService) {
    this.dependencyDao = dependencyDao;
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap map = context.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    // Delete dependencies from Source Dataset's Firestore
    dependencyDao.deleteSnapshotFileDependencies(dataset, snapshotId.toString());

    // Delete View ACLs from Source Dataset's Big Query Tables
    try {
      bigQuerySnapshotPdao.deleteSourceDatasetViewACLs(snapshot);
    } catch (BigQueryException ex) {
      if (FlightUtils.isBigQueryIamPropagationError(ex)) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new PdaoException("Caught BQ exception while deleting snapshot", ex));
    }

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
