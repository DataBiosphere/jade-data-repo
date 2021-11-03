package bio.terra.service.dataset.flight.ingest;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OptionalStep implements Step {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final Predicate<FlightContext> doStepCondition;

  public OptionalStep() {
    this(OptionalStep::alwaysDo);
  }

  public OptionalStep(Predicate<FlightContext> doCondition) {
    this.doStepCondition = doCondition;
  }

  protected static boolean alwaysDo(FlightContext flightContext) {
    return true;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    if (doStepCondition.test(flightContext)) {
      return doOptionalStep(flightContext);
    }
    logger.info("Skipping {} step based on flight context state", this.getClass().getName());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    if (doStepCondition.test(flightContext)) {
      return undoOptionalStep(flightContext);
    }
    logger.info("Skipping {} undo step based on flight context state", this.getClass().getName());
    return StepResult.getStepResultSuccess();
  }

  public abstract StepResult doOptionalStep(FlightContext flightContext)
      throws InterruptedException, RetryException;

  public StepResult undoOptionalStep(FlightContext context) {
    // This step has no side effects
    return StepResult.getStepResultSuccess();
  }
}
