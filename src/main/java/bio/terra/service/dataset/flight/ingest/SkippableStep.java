package bio.terra.service.dataset.flight.ingest;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SkippableStep implements Step {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final Predicate<FlightContext> skipCondition;

  public SkippableStep() {
    this(x -> false);
  }

  public SkippableStep(Predicate<FlightContext> skipCondition) {
    this.skipCondition = skipCondition;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    if (skipCondition.test(flightContext)) {
      logger.info("Skipping {} step based on flight context state", this.getClass().getName());
      return StepResult.getStepResultSuccess();
    }
    return doSkippableStep(flightContext);
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    if (skipCondition.test(flightContext)) {
      logger.info("Skipping {} undo step based on flight context state", this.getClass().getName());
      return StepResult.getStepResultSuccess();
    }
    return undoSkippableStep(flightContext);
  }

  public abstract StepResult doSkippableStep(FlightContext flightContext)
      throws InterruptedException;

  public StepResult undoSkippableStep(FlightContext context) {
    // This step has no side effects
    return StepResult.getStepResultSuccess();
  }

  protected static boolean neverSkip(FlightContext flightContext) {
    return false;
  }
}
