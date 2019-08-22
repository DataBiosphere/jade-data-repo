package bio.terra.filesystem.flight.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreObject;
import bio.terra.filesystem.FireStoreUtils;
import bio.terra.filesystem.exception.FileSystemObjectAlreadyExistsException;
import bio.terra.filesystem.flight.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.model.FileLoadModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;

public class IngestFileDirectoryStep implements Step {
    private final FireStoreDao fileDao;
    private final FireStoreUtils fireStoreUtils;
    private final Dataset dataset;

    public IngestFileDirectoryStep(FireStoreDao fileDao,
                                   FireStoreUtils fireStoreUtils,
                                   Dataset dataset) {
        this.fileDao = fileDao;
        this.fireStoreUtils = fireStoreUtils;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel loadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);

        String datasetId = dataset.getId().toString();
        String targetPath = loadModel.getTargetPath();

        // Lookup the file - on a recovery, we may have already created it, but not
        // finished. Or it might already exist, created by someone else.
        FireStoreObject existingObject = fileDao.retrieveDirectoryEntryByPath(dataset, targetPath);
        if (existingObject == null) {
            // Not there - create it
            FireStoreObject newObject = new FireStoreObject()
                .objectId(objectId)
                .fileRef(true)
                .path(fireStoreUtils.getDirectoryPath(loadModel.getTargetPath()))
                .name(fireStoreUtils.getObjectName((loadModel.getTargetPath())))
                .datasetId(datasetId);
            fileDao.createDirectoryEntry(dataset, newObject);
        } else {
            if (!StringUtils.equals(existingObject.getObjectId(), objectId)) {
                // Exists, but is not what we are trying to create
                throw new FileSystemObjectAlreadyExistsException("Path already exists: " + targetPath);
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        fileDao.deleteDirectoryEntry(dataset, objectId);
        return StepResult.getStepResultSuccess();
    }

}
