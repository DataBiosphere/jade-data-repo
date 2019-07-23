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
    private final String studyId;
    private final StudyService studyService;

    public DeleteFileMetadataStepStart(String studyId,
                                       FireStoreFileDao fileDao,
                                       String fileId,
                                       StudyService studyService) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.studyId = studyId;
        this.studyService = studyService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = studyService.retrieve(UUID.fromString(studyId));
        fileDao.deleteFileStart(study, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Study study = studyService.retrieve(UUID.fromString(studyId));
        fileDao.deleteFileStartUndo(study, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
