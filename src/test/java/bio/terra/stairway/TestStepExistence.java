package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TestStepExistence implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");
    private String filename;

    public TestStepExistence(String filename) {
        this.filename = filename;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        logger.debug("TestStepExistence");
        File file = new File(filename);

        if (file.exists()) {
            logger.debug("File " + filename + " already exists");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                    new IllegalArgumentException("File " + filename + " already exists."));
        }

        logger.debug("File " + filename + " does not exist");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to UNDO, since the DO has only implicit persistent results
        return StepResult.getStepResultSuccess();
    }

}
