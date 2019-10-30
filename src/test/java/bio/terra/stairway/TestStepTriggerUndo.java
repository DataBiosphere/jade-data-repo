package bio.terra.stairway;

public class TestStepTriggerUndo implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
        // This step sets the stop controller to 0 to cause the
        // stop step to sleep. Then it returns a fatal error.
        TestStopController.setControl(0);
        throw new RuntimeException("TestStepTriggerUndo");
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
