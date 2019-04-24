package bio.terra.flight.file.delete;

import bio.terra.filesystem.FileDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteFileMetadataStepStart implements Step {
    private final FileDao fileDao;
    private final UUID fileId;

    public DeleteFileMetadataStepStart(FileDao fileDao, String fileId) {
        this.fileDao = fileDao;
        this.fileId = UUID.fromString(fileId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileStart(fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        fileDao.deleteFileStartUndo(fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
