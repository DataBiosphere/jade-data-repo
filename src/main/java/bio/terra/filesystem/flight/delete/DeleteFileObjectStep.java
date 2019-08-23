package bio.terra.filesystem.flight.delete;

import bio.terra.filesystem.FireStoreDirectoryDao;
import bio.terra.metadata.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileObjectStep implements Step {
    private final FireStoreDirectoryDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileObjectStep(FireStoreDirectoryDao fileDao,
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
