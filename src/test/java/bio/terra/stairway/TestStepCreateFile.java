package bio.terra.stairway;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestStepCreateFile implements Step {
    private String filename;
    private String text;

    public TestStepCreateFile(String filename, String text) {
        this.filename = filename;
        this.text = text;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We assume the file does not exist, since that was checked in the previous step
        // The createFileMetadata will fail if the file exists.
        try {
            Path filepath = Paths.get(filename);
            Files.createFile(filepath);
            Files.write(filepath, text.getBytes("UTF-8"));
            return StepResult.getStepResultSuccess();
        } catch (IOException | RuntimeException ex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }

    }

    @Override
    public StepResult undoStep(FlightContext context) {
        File file = new File(filename);
        // Non-existent file is not an error; failing to delete an existing file is
        if (file.exists() && !file.delete()) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                    new IllegalArgumentException("Failed to delete File " + filename));
        }
        return StepResult.getStepResultSuccess();
    }
}
