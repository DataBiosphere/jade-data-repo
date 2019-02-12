package bio.terra.stairway;

import bio.terra.stairway.exception.FlightException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepRetry implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    private int timesToFail;
    private int timesFailed;

    public TestStepRetry(int timesToFail) {
        this.timesToFail = timesToFail;
        this.timesFailed = 0;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        logger.debug("TestStepRetry - timesFailed=" + timesFailed + " timesToFail=" + timesToFail);

        if (timesFailed < timesToFail) {
            timesFailed++;
            logger.debug(" - failure_retry");
            Throwable throwable = new FlightException("step retry failed");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, throwable);
        }
        logger.debug(" - success");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
