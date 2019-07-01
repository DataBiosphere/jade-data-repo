package bio.terra.flight.file.delete;

import bio.terra.dao.DrDatasetDao;
import bio.terra.metadata.DrDataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteFilePrimaryDataStep implements Step {
    private final DrDatasetDao datasetDao;
    private final String datasetId;
    private final String fileId;
    private final GcsPdao gcsPdao;

    public DeleteFilePrimaryDataStep(DrDatasetDao datasetDao, String datasetId, String fileId, GcsPdao gcsPdao) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = datasetDao.retrieve(UUID.fromString(datasetId));
        gcsPdao.deleteFile(dataset, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
