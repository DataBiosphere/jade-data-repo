package bio.terra.stairway;

import bio.terra.stairway.exception.FlightException;

import static bio.terra.stairway.TestUtil.debugWrite;

public class TestStepRetry implements Step {
    private int timesToFail;
    private int timesFailed;

    public TestStepRetry(int timesToFail) {
        this.timesToFail = timesToFail;
        this.timesFailed = 0;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        debugWrite("TestStepRetry - timesFailed=" + timesFailed + " timesToFail=" + timesToFail);

        if (timesFailed < timesToFail) {
            timesFailed++;
            debugWrite(" - failure_retry");
            Throwable throwable = new FlightException("step retry failed");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, throwable);
        }
        debugWrite(" - success");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
