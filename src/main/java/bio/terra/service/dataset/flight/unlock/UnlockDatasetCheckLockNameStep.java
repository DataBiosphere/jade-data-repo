package bio.terra.service.dataset.flight.unlock;

import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class UnlockDatasetCheckLockNameStep extends DefaultUndoStep {
  private final DatasetService datasetService;
  private final UUID datasetId;
  private final String lockName;

  public UnlockDatasetCheckLockNameStep(
      DatasetService datasetService, UUID datasetId, String lockName) {
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.lockName = lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    // Is the dataset actually locked by this flight?
    String exclusiveLock = "";
    try {
      DatasetSummaryModel response = datasetService.retrieveDatasetSummary(datasetId);
      exclusiveLock = response.getResourceLocks().getExclusive();
    } catch (Exception ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    if (!exclusiveLock.equals(lockName)) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetLockException("Dataset is not locked by lock name: " + lockName));
    }
    return StepResult.getStepResultSuccess();
  }
}
