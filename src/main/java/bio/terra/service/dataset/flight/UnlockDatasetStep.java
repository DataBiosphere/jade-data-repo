package bio.terra.service.dataset.flight;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockDatasetStep extends OptionalStep {

  private static Logger logger = LoggerFactory.getLogger(UnlockDatasetStep.class);

  private final DatasetService datasetService;
  private boolean sharedLock; // default to false
  private UUID datasetId;

  public UnlockDatasetStep(
      DatasetService datasetService,
      UUID datasetId,
      boolean sharedLock,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.datasetService = datasetService;
    this.datasetId = datasetId;

    // this will be set to true for a shared lock, false for an exclusive lock
    this.sharedLock = sharedLock;
  }

  public UnlockDatasetStep(DatasetService datasetService, UUID datasetId, boolean sharedLock) {
    this(datasetService, datasetId, sharedLock, OptionalStep::alwaysDo);
  }

  public UnlockDatasetStep(DatasetService datasetService, boolean sharedLock) {
    this(datasetService, null, sharedLock, OptionalStep::alwaysDo);
  }

  public UnlockDatasetStep(
      DatasetService datasetService, boolean sharedLock, Predicate<FlightContext> doCondition) {
    this(datasetService, null, sharedLock, doCondition);
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
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

    try {
      datasetService.unlock(datasetId, context.getFlightId(), sharedLock);
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
