package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileMetadataStepStart implements Step {
    private final FireStoreFileDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileMetadataStepStart(FireStoreFileDao fileDao,
                                       String fileId,
                                       Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileStart(dataset, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        fileDao.deleteFileStartUndo(dataset, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
