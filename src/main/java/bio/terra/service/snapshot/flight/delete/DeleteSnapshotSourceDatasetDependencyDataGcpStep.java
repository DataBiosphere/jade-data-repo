package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryException;
import java.util.UUID;
import java.util.function.Predicate;

public class DeleteSnapshotSourceDatasetDependencyDataGcpStep extends OptionalStep {
  private final FireStoreDependencyDao dependencyDao;
  private final UUID snapshotId;
  private final DatasetService datasetService;

  public DeleteSnapshotSourceDatasetDependencyDataGcpStep(
      FireStoreDependencyDao dependencyDao,
      UUID snapshotId,
      DatasetService datasetService,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.dependencyDao = dependencyDao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) throws InterruptedException {
    FlightMap map = context.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    try {
      Dataset dataset = datasetService.retrieve(datasetId);
      dependencyDao.deleteSnapshotFileDependencies(dataset, snapshotId.toString());

    } catch (BigQueryException ex) {
      if (FlightUtils.isBigQueryIamPropagationError(ex)) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw new PdaoException("Caught BQ exception while deleting snapshot", ex);
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
