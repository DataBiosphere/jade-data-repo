package bio.terra.stairway;

import java.io.File;

public class TestStepExistence implements Step {
    private static final boolean debugOutput = false;
    private String filename;

    public TestStepExistence(String filename) {
        this.filename = filename;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        debugWrite("TestStepExistence");
        File file = new File(filename);

        if (file.exists()) {
            debugWrite("File " + filename + " already exists");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                    new IllegalArgumentException("File " + filename + " already exists."));
        }

        debugWrite("File " + filename + " does not exist");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to UNDO, since the DO has only implicit persistent results
        return StepResult.getStepResultSuccess();
    }

    private void debugWrite(String msg) {
        if (debugOutput) {
            System.out.println(msg);
        }
    }

}
