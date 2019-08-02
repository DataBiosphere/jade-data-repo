package bio.terra.service;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class JobServiceTestFlight extends Flight {

    public JobServiceTestFlight(FlightMap inputParameters, Object applicationContext, AuthenticatedUser testUser) {
        super(inputParameters, applicationContext, testUser);

        // Pull out our parameters and feed them in to the step classes.
        String description = inputParameters.get("description", String.class);

        // Just one step for this test
        addStep(new JobServiceTestStep(description));
    }

}
