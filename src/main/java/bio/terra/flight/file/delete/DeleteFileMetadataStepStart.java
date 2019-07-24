package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Study;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteFileMetadataStepStart implements Step {
    private final FireStoreFileDao fileDao;
    private final String fileId;
    private final Study study;

    public DeleteFileMetadataStepStart(FireStoreFileDao fileDao,
                                       String fileId,
                                       Study study) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.study = study;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileStart(study, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        fileDao.deleteFileStartUndo(study, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
