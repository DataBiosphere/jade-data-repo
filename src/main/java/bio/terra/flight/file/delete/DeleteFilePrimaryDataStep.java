package bio.terra.flight.file.delete;

import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;


public class DeleteFilePrimaryDataStep implements Step {
    private final Dataset dataset;
    private final String fileId;
    private final GcsPdao gcsPdao;

    public DeleteFilePrimaryDataStep(Dataset dataset, String fileId, GcsPdao gcsPdao) {
        this.dataset = dataset;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        gcsPdao.deleteFile(dataset, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
