package bio.terra.stairway;

import java.io.File;

public class TestStepExistence implements Step {
    private String filename;

    public TestStepExistence(String filename) {
        this.filename = filename;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        System.out.println("TestStepExistence");
        File file = new File(filename);

        if (file.exists()) {
            System.out.println("File " + filename + " already exists");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                    new IllegalArgumentException("File " + filename + " already exists."));
        }

        System.out.println("File " + filename + " does not exist");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to UNDO, since the DO has only implicit persistent results
        return StepResult.getStepResultSuccess();
    }
}
