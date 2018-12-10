package bio.terra.stairway;

public class TestStepRetry implements Step {
    private int timesToFail;
    private int timesFailed;

    public TestStepRetry(int timesToFail) {
        this.timesToFail = timesToFail;
        this.timesFailed = 0;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        System.out.print("TestStepRetry - timesFailed=" + timesFailed + " timesToFail=" + timesToFail);
        if (timesFailed < timesToFail) {
            timesFailed++;
            System.out.println(" - failure_retry");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
        }
        System.out.println(" - success");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
