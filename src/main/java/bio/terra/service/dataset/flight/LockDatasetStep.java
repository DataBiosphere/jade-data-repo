package bio.terra.service.dataset.flight;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockDatasetStep extends OptionalStep {

  private static Logger logger = LoggerFactory.getLogger(LockDatasetStep.class);

  private final DatasetService datasetService;
  private final UUID datasetId;
  private final boolean sharedLock; // default to false
  private final boolean suppressNotFoundException; // default to false

  public LockDatasetStep(DatasetService datasetService, UUID datasetId, boolean sharedLock) {
    this(datasetService, datasetId, sharedLock, false, OptionalStep::alwaysDo);
  }

  public LockDatasetStep(
      DatasetService datasetService,
      UUID datasetId,
      boolean sharedLock,
      boolean suppressNotFoundException) {
    this(datasetService, datasetId, sharedLock, suppressNotFoundException, OptionalStep::alwaysDo);
  }

  public LockDatasetStep(
      DatasetService datasetService,
      UUID datasetId,
      boolean sharedLock,
      Predicate<FlightContext> doCondition) {
    this(datasetService, datasetId, sharedLock, false, doCondition);
  }

  public LockDatasetStep(
      DatasetService datasetService,
      UUID datasetId,
      boolean sharedLock,
      boolean suppressNotFoundException,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.datasetService = datasetService;
    this.datasetId = datasetId;

    // this will be set to true for a shared lock, false for an exclusive lock
    this.sharedLock = sharedLock;

    // this will be set to true in cases where we don't want to fail if the dataset metadata record
    // doesn't exist.
    // for example, dataset deletion. we want multiple deletes to succeed, not throw a lock or
    // notfound exception.
    // for most cases, this should be set to false because we expect the dataset metadata record to
    // exist.
    this.suppressNotFoundException = suppressNotFoundException;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {

    try {
      datasetService.lockDataset(datasetId, context.getFlightId(), sharedLock);
      return StepResult.getStepResultSuccess();
    } catch (DatasetNotFoundException notFoundEx) {
      if (suppressNotFoundException) {
        logger.debug("Suppressing DatasetNotFoundException");
        return StepResult.getStepResultSuccess();
      } else {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, notFoundEx);
      }
    } catch (RetryQueryException | DatasetLockException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // try to unlock the flight if something went wrong above
    // note the unlock will only clear the flightid if it's set to this flightid
    String flightId = context.getFlightId();
    try {
      datasetService.unlockDataset(datasetId, flightId, sharedLock);
      return StepResult.getStepResultSuccess();
    } catch (DatasetLockException e) {
      // DatasetLockException will be thrown if flight id was not set
      return StepResult.getStepResultSuccess();
    } catch (DatasetNotFoundException notFoundEx) {
      if (suppressNotFoundException) {
        logger.debug("Suppressing DatasetNotFoundException");
        return StepResult.getStepResultSuccess();
      } else {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, notFoundEx);
      }
    } catch (RetryQueryException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
  }
}
