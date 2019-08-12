package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;


public class DeleteFilePrimaryDataStep implements Step {
    private final Dataset dataset;
    private final String fileId;
    private final GcsPdao gcsPdao;
    private final FireStoreFileDao fileDao;

    public DeleteFilePrimaryDataStep(Dataset dataset, String fileId, GcsPdao gcsPdao, FireStoreFileDao fileDao) {
        this.dataset = dataset;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FSObjectBase fsObject = fileDao.retrieve(dataset, UUID.fromString(fileId));
        if (fsObject.getObjectType() != FSObjectType.DELETING_FILE) {
            throw new FileSystemCorruptException("This should be a file we're deleting!");
        }
        gcsPdao.deleteFile((FSFile)fsObject);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
