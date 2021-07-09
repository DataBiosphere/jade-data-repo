package bio.terra.datarepo.service.dataset.flight;

import bio.terra.datarepo.common.exception.RetryQueryException;
import bio.terra.datarepo.service.dataset.DatasetDao;
import bio.terra.datarepo.service.dataset.exception.DatasetLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockDatasetStep implements Step {

  private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

  private final DatasetDao datasetDao;
  private UUID datasetId;
  private boolean sharedLock; // default to false

  public UnlockDatasetStep(DatasetDao datasetDao, UUID datasetId, boolean sharedLock) {
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;

    // this will be set to true for a shared lock, false for an exclusive lock
    this.sharedLock = sharedLock;
  }

  public UnlockDatasetStep(DatasetDao datasetDao, boolean sharedLock) {
    this(datasetDao, null, sharedLock);
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // In the create case, we won't have the dataset id at step creation. We'll expect it to be in
    // the working map.
    if (datasetId == null) {
      datasetId = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
      if (datasetId == null) {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new DatasetLockException("Expected dataset id in working map."));
      }
    }

    boolean rowUpdated;
    try {
      if (sharedLock) {
        rowUpdated = datasetDao.unlockShared(datasetId, context.getFlightId());
      } else {
        rowUpdated = datasetDao.unlockExclusive(datasetId, context.getFlightId());
      }
      logger.debug("rowUpdated on unlock = " + rowUpdated);
    } catch (RetryQueryException retryQueryException) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
