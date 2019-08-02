package bio.terra.stairway;

import bio.terra.controller.AuthenticatedUser;

public class TestFlightRecoveryUndo extends Flight {

    public TestFlightRecoveryUndo(FlightMap inputParameters, Object applicationContext, AuthenticatedUser testUser) {
        super(inputParameters, applicationContext, testUser);

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
