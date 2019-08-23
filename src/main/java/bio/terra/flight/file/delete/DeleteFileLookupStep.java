package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreFile;
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

    public DeleteFileLookupStep(FireStoreDao fileDao,
                                String fileId,
                                Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
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
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
