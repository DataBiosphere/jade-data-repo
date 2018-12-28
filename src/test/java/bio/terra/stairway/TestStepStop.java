package bio.terra.stairway;

import java.util.concurrent.TimeUnit;

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
            System.out.println("TestStepStop stopping");
            try {
                TimeUnit.HOURS.sleep(1);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
            }
        }
        System.out.println("TestStepStop did not stop");
        return StepResult.getStepResultSuccess();
    }
}
