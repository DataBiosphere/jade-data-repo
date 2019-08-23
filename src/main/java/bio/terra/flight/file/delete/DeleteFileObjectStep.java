package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.metadata.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileObjectStep implements Step {
    private final FireStoreDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileObjectStep(FireStoreDao fileDao,
                                String fileId,
                                Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileObject(dataset, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No possible undo
        return StepResult.getStepResultSuccess();
    }

}
