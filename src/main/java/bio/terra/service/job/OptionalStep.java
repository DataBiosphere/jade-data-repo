package bio.terra.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OptionalStep extends Step {

  Logger logger = LoggerFactory.getLogger(OptionalStep.class);

  Step step();

  boolean isEnabled(FlightContext context);

  default String getSkipReason() {
    return "of flight context state";
  }

  default String getRunReason(FlightContext context) {
    return "of flight context state";
  }

  @Override
  default StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    if (isEnabled(context)) {
      logger.info("Running {} because {}", step().getClass().getName(), getRunReason(context));
      return step().doStep(context);
    }
    logger.info("Skipping {} because {}", step().getClass().getName(), getSkipReason());
    return StepResult.getStepResultSuccess();
  }

  @Override
  default StepResult undoStep(FlightContext context) throws InterruptedException {
    if (isEnabled(context)) {
      logger.info("Running {} undo because {}", step().getClass().getName(), getRunReason(context));
      return step().undoStep(context);
    }
    logger.info("Skipping {} undo because {}", step().getClass().getName(), getSkipReason());
    return StepResult.getStepResultSuccess();
  }
}
