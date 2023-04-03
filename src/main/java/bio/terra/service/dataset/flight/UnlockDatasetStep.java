package bio.terra.service.dataset.flight;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.springframework.transaction.TransactionSystemException;

public record UnlockDatasetStep(DatasetService datasetService, UUID datasetId, boolean sharedLock)
    implements DefaultUndoStep {

  public UnlockDatasetStep(DatasetService datasetService, boolean sharedLock) {
    this(datasetService, null, sharedLock);
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    final UUID foundDatasetId;
    if (datasetId != null) {
      foundDatasetId = datasetId;
    } else {
      // In the create case, we won't have the dataset id at step creation. We'll expect it to be in
      // the working map.
      if (map.containsKey(DatasetWorkingMapKeys.DATASET_ID)) {
        foundDatasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
      } else {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new DatasetLockException(
                "Expected dataset id to either be passed in or in the working map."));
      }
    }

    try {
      datasetService.unlock(foundDatasetId, context.getFlightId(), sharedLock);
    } catch (RetryQueryException | TransactionSystemException retryQueryException) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    }
    return StepResult.getStepResultSuccess();
  }
}
