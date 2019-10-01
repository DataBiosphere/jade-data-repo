package bio.terra.flight.file.delete;

import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.metadata.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileMetadataStep implements Step {
    private final FireStoreDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileMetadataStep(FireStoreDao fileDao,
                                  String fileId,
                                  Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteFileMetadata(dataset, fileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No possible undo
        return StepResult.getStepResultSuccess();
    }

}
