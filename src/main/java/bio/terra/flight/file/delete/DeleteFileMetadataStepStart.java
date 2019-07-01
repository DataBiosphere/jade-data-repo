package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileMetadataStepStart implements Step {
    private final FireStoreFileDao fileDao;
    private final String fileId;
    private final String datasetId;

    public DeleteFileMetadataStepStart(String datasetId, FireStoreFileDao fileDao, String fileId) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileStart(datasetId, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        fileDao.deleteFileStartUndo(datasetId, fileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
