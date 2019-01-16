package bio.terra.stairway;

public class TestFlightRecoveryUndo extends Flight {

    public TestFlightRecoveryUndo(FlightMap inputParameters) {
        super(inputParameters);

        // Step 0 - increment
        addStep(new TestStepIncrement());

        // Step 1 - stop - allow for failure
        addStep(new TestStepStop());

        // Step 2 - increment
        addStep(new TestStepIncrement());

        // Step 3 - trigger undo
        addStep(new TestStepTriggerUndo());
    }

}
