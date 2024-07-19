package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional steps are used to optionally run or skip their nested step based on information that is
 * generated as the flight is run rather than information available on flight construction
 * (information provided in an api request). We use them to optionally run steps based on
 * information put in the flight map by other steps.
 */
public abstract class OptionalStep implements Step {

  private static final Logger logger = LoggerFactory.getLogger(OptionalStep.class);

  private final Step step;

  public OptionalStep(Step step) {
    this.step = step;
  }

  @VisibleForTesting
  public Step getStep() {
    return step;
  }

  public abstract boolean isEnabled(FlightContext context);

  public String getSkipReason() {
    return "of flight context state";
  }

  public String getRunReason(FlightContext context) {
    return "of flight context state";
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    if (isEnabled(flightContext)) {
      logger.info("Running {} because {}", step.getClass().getName(), getRunReason(flightContext));
      return step.doStep(flightContext);
    }
    logger.info("Skipping {} because {}", step.getClass().getName(), getSkipReason());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    if (isEnabled(flightContext)) {
      logger.info(
          "Running {} undo because {}", step.getClass().getName(), getRunReason(flightContext));
      return step.undoStep(flightContext);
    }
    logger.info("Skipping {} undo because {}", step.getClass().getName(), getSkipReason());
    return StepResult.getStepResultSuccess();
  }
}
