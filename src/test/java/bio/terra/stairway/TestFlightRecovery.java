package bio.terra.stairway;

import bio.terra.controller.UserInfo;

public class TestFlightRecovery extends Flight {

    public TestFlightRecovery(FlightMap inputParameters, Object applicationContext, UserInfo testUser) {
        super(inputParameters, applicationContext, testUser);

        // Step 1 - increment
        addStep(new TestStepIncrement());

        // Step 2 - stop - allow for failure
        addStep(new TestStepStop());

        // Step 3 - increment
        addStep(new TestStepIncrement());
    }

}
