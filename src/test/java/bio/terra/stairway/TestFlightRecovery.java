package bio.terra.stairway;

public class TestFlightRecovery extends Flight {

    public TestFlightRecovery(FlightMap inputParameters) {
        super(inputParameters);

        // Step 1 - increment
        addStep(new TestStepIncrement());

        // Step 2 - stop - allow for failure
        addStep(new TestStepStop());

        // Step 3 - increment
        addStep(new TestStepIncrement());
    }

}
