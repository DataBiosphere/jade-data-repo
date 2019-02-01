package bio.terra.stairway;

import java.util.concurrent.TimeUnit;

import static bio.terra.stairway.TestUtil.debugWrite;

public class TestStepStop implements Step {

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
            debugWrite("TestStepStop stopping");
            try {
                TimeUnit.HOURS.sleep(1);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
            }
        }
        debugWrite("TestStepStop did not stop");
        return StepResult.getStepResultSuccess();
    }
}
