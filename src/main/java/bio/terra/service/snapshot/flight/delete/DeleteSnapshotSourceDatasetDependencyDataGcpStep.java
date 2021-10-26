package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryException;
import java.util.UUID;

public class DeleteSnapshotSourceDatasetDependencyDataGcpStep implements Step {
  private FireStoreDependencyDao dependencyDao;
  private UUID snapshotId;
  private DatasetService datasetService;
  private UUID datasetId;

  public DeleteSnapshotSourceDatasetDependencyDataGcpStep(
      FireStoreDependencyDao dependencyDao,
      UUID snapshotId,
      DatasetService datasetService,
      UUID datasetId) {
    this.dependencyDao = dependencyDao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    try {
      Dataset dataset = datasetService.retrieve(datasetId);
      dependencyDao.deleteSnapshotFileDependencies(dataset, snapshotId.toString());

    } catch (BigQueryException ex) {
      if (FlightUtils.isBigQueryIamPropagationError(ex)) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw new PdaoException("Caught BQ exception while deleting snapshot", ex);
    } catch (DatasetNotFoundException nfe) {
      // If we do not find the snapshot or dataset, we assume things are already clean
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
