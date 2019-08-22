package bio.terra.stairway;

public class TestFlight extends Flight {

    public TestFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo testUser) {
        super(inputParameters, applicationContext, testUser);

        // Pull out our parameters and feed them in to the step classes.
        String filename = inputParameters.get("filename", String.class);
        String text = inputParameters.get("text", String.class);

        // Step 1 - test file existence
        addStep(new TestStepExistence(filename));

        // Step 2 - create file
        addStep(new TestStepCreateFile(filename, text));
    }

}
