package bio.terra.flight.file.delete;

import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.filedata.google.firestore.FireStoreFile;
import bio.terra.filedata.exception.FileDependencyException;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFileLookupStep implements Step {
    private final FireStoreDao fileDao;
    private final String fileId;
    private final Dataset dataset;
    private final FireStoreDependencyDao dependencyDao;

    public DeleteFileLookupStep(FireStoreDao fileDao,
                                String fileId,
                                Dataset dataset,
                                FireStoreDependencyDao dependencyDao) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
        this.dependencyDao = dependencyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // If we are restarting, we may have already retrieved and saved the file,
        // so we check the working map before doing the lookup.
        FlightMap workingMap = context.getWorkingMap();
        FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
        if (fireStoreFile == null) {
            fireStoreFile = fileDao.lookupFile(dataset, fileId);
            if (fireStoreFile != null) {
                workingMap.put(FileMapKeys.FIRESTORE_FILE, fireStoreFile);
            }
        }
        // We may not have found a fireStoreFile either way. We don't have a way to short-circuit
        // running the rest of the steps, so we use the null stored in the working map to let other
        // steps know there is no file. If there is a file, check dependencies here.
        if (fireStoreFile != null) {
            if (dependencyDao.fileHasSnapshotReference(dataset, fireStoreFile.getFileId())) {
                throw new FileDependencyException(
                    "File is used by at least one snapshot and cannot be deleted");
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
