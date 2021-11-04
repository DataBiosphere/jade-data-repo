package bio.terra.service.dataset.flight;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockDatasetStep extends OptionalStep {

  private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

  private final DatasetDao datasetDao;
  private boolean sharedLock; // default to false
  private UUID datasetId;

  public UnlockDatasetStep(
      DatasetDao datasetDao,
      UUID datasetId,
      boolean sharedLock,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;

    // this will be set to true for a shared lock, false for an exclusive lock
    this.sharedLock = sharedLock;
  }

  public UnlockDatasetStep(DatasetDao datasetDao, UUID datasetId, boolean sharedLock) {
    this(datasetDao, datasetId, sharedLock, OptionalStep::alwaysDo);
  }

  public UnlockDatasetStep(DatasetDao datasetDao, boolean sharedLock) {
    this(datasetDao, null, sharedLock, OptionalStep::alwaysDo);
  }

  public UnlockDatasetStep(
      DatasetDao datasetDao, boolean sharedLock, Predicate<FlightContext> doCondition) {
    this(datasetDao, null, sharedLock, doCondition);
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
    boolean rowUpdated;
    FlightMap map = context.getWorkingMap();
    try {
      // In the create case, we won't have the dataset id at step creation. We'll expect it to be in
      // the working map.
      datasetId =
          Objects.requireNonNullElse(
              datasetId, map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class));
      if (sharedLock) {
        rowUpdated = datasetDao.unlockShared(datasetId, context.getFlightId());
      } else {
        rowUpdated = datasetDao.unlockExclusive(datasetId, context.getFlightId());
      }
      logger.debug("rowUpdated on unlock = " + rowUpdated);
    } catch (RetryQueryException retryQueryException) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    } catch (NullPointerException nullPointerException) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetLockException(
              "Expected dataset id to either be passed in or in the working map."));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
