package bio.terra.stairway;

public class TestStepRetry implements Step {
    static final boolean debugOutput = false;

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
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
        }
        debugWrite(" - success");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

    private void debugWrite(String msg) {
        if (debugOutput) {
            System.out.println(msg);
        }
    }

}
