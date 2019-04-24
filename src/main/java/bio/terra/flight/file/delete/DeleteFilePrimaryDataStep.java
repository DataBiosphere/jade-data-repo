package bio.terra.flight.file.delete;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteFilePrimaryDataStep implements Step {
    private final StudyDao studyDao;
    private final String studyId;
    private final String fileId;
    private final GcsPdao gcsPdao;

    public DeleteFilePrimaryDataStep(StudyDao studyDao, String studyId, String fileId, GcsPdao gcsPdao) {
        this.studyDao = studyDao;
        this.studyId = studyId;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = studyDao.retrieve(UUID.fromString(studyId));
        gcsPdao.deleteFile(study, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
