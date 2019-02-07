package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TestStepStop implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");


    @Override
    public StepResult doStep(FlightContext context) {
        return sleepStop();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return sleepStop();
    }

    private StepResult sleepStop() {
        if (TestStopController.getControl() == 0) {
            logger.debug("TestStepStop stopping");
            try {
                TimeUnit.HOURS.sleep(1);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
            }
        }
        logger.debug("TestStepStop did not stop");
        return StepResult.getStepResultSuccess();
    }
}
