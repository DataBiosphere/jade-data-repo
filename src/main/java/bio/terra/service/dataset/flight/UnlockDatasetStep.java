package bio.terra.service.dataset.flight;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.springframework.transaction.TransactionSystemException;

public class UnlockDatasetStep extends DefaultUndoStep {

  private final DatasetService datasetService;
  private final boolean sharedLock;
  private UUID datasetId;
  private String lockName;

  public UnlockDatasetStep(DatasetService datasetService, UUID datasetId, boolean sharedLock) {
    this.datasetService = datasetService;
    this.datasetId = datasetId;

    // this will be set to true for a shared lock, false for an exclusive lock
    this.sharedLock = sharedLock;
  }

  public UnlockDatasetStep(DatasetService datasetService, boolean sharedLock) {
    this(datasetService, null, sharedLock);
  }

  public UnlockDatasetStep(
      DatasetService datasetService, UUID datasetId, boolean sharedLock, String lockName) {
    this(datasetService, datasetId, sharedLock);
    this.lockName = lockName;
  }

  @VisibleForTesting
  public boolean isSharedLock() {
    return sharedLock;
  }

  @VisibleForTesting
  public String getLockName() {
    return lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    if (datasetId == null) {
      // In the create case, we won't have the dataset id at step creation. We'll expect it to be in
      // the working map.
      if (map.containsKey(DatasetWorkingMapKeys.DATASET_ID)) {
        datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
      } else {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new DatasetLockException(
                "Expected dataset id to either be passed in or in the working map."));
      }
    }
    if (lockName == null) {
      lockName = context.getFlightId();
    }

    try {
      datasetService.unlock(datasetId, lockName, sharedLock);
    } catch (RetryQueryException | TransactionSystemException retryQueryException) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    }
    return StepResult.getStepResultSuccess();
  }
}
