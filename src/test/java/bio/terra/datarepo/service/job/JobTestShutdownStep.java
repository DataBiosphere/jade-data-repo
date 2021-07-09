package bio.terra.datarepo.service.job;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTestShutdownStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(JobTestShutdownStep.class);
  private int flightWaitSeconds;

  public JobTestShutdownStep(int flightWaitSeconds) {
    this.flightWaitSeconds = flightWaitSeconds;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    logger.info("Starting do flightid: " + flightContext.getFlightId());
    TimeUnit.SECONDS.sleep(flightWaitSeconds);
    logger.info("Completing do flightid: " + flightContext.getFlightId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
