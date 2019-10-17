package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.filedata.exception.FileAlreadyExistsException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.FileLoadModel;
import bio.terra.service.job.JobMapKeys;
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
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

        String datasetId = dataset.getId().toString();
        String targetPath = loadModel.getTargetPath();

        // Lookup the file - on a recovery, we may have already created it, but not
        // finished. Or it might already exist, created by someone else.
        FireStoreDirectoryEntry existingEntry = fileDao.lookupDirectoryEntryByPath(dataset, targetPath);
        if (existingEntry == null) {
            // Not there - create it
            FireStoreDirectoryEntry newEntry = new FireStoreDirectoryEntry()
                .fileId(fileId)
                .isFileRef(true)
                .path(fireStoreUtils.getDirectoryPath(loadModel.getTargetPath()))
                .name(fireStoreUtils.getName((loadModel.getTargetPath())))
                .datasetId(datasetId);
            fileDao.createDirectoryEntry(dataset, newEntry);
        } else {
            if (!StringUtils.equals(existingEntry.getFileId(), fileId)) {
                // Exists, but is not our file!
                throw new FileAlreadyExistsException("Path already exists: " + targetPath);
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
        fileDao.deleteDirectoryEntry(dataset, fileId);
        return StepResult.getStepResultSuccess();
    }

}
