package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileMetadataStepStart implements Step {
    private final FireStoreFileDao fileDao;
    private final String fileId;
    private final String studyId;

    public DeleteFileMetadataStepStart(String studyId, FireStoreFileDao fileDao, String fileId) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.studyId = studyId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileStart(studyId, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        fileDao.deleteFileStartUndo(studyId, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
