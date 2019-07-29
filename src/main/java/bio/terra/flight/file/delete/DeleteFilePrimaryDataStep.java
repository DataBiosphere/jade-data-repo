package bio.terra.flight.file.delete;

import bio.terra.metadata.Study;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;


public class DeleteFilePrimaryDataStep implements Step {
    private final Study study;
    private final String fileId;
    private final GcsPdao gcsPdao;

    public DeleteFilePrimaryDataStep(Study study, String fileId, GcsPdao gcsPdao) {
        this.study = study;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        gcsPdao.deleteFile(study, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
