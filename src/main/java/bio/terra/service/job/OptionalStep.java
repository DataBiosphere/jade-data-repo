package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OptionalStep implements Step {

  private static final Logger logger = LoggerFactory.getLogger(OptionalStep.class);

  private final Step step;

  public OptionalStep(Step step) {
    this.step = step;
  }

  public abstract boolean isEnabled(FlightContext context);

  public String getReason() {
    return "of flight context state";
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    if (isEnabled(flightContext)) {
      logger.info("Running {} because {}", step.getClass().getName(), getReason());
      return step.doStep(flightContext);
    }
    logger.info("Skipping {} because {}", step.getClass().getName(), getReason());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    if (isEnabled(flightContext)) {
      logger.info("Running {} undo because {}", step.getClass().getName(), getReason());
      return step.undoStep(flightContext);
    }
    logger.info("Skipping {} undo because {}", step.getClass().getName(), getReason());
    return StepResult.getStepResultSuccess();
  }
}
