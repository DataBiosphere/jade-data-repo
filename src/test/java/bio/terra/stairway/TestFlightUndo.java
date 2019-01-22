package bio.terra.stairway;

public class TestFlightUndo extends Flight {

    public TestFlightUndo(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // Pull out our parameters and feed them in to the step classes.
        String filename = inputParameters.get("filename", String.class);
        String existingFilename = inputParameters.get("existingFilename", String.class);
        String text = inputParameters.get("text", String.class);

        // Step 1 - test file existence
        addStep(new TestStepExistence(filename));

        // Step 2 - create file
        addStep(new TestStepCreateFile(filename, text));

        // Step 3 - test file existence - should fail
        addStep(new TestStepExistence(existingFilename));

        // Step 4 - should not get here
        addStep(new TestStepCreateFile(existingFilename, text));
    }

}
