package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemObjectAlreadyExistsException;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.DrDataset;
import bio.terra.model.FileLoadModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class IngestFileMetadataStepStart implements Step {
    private final FireStoreFileDao fileDao;
    private final DrDataset dataset;

    public IngestFileMetadataStepStart(FireStoreFileDao fileDao, DrDataset dataset) {
        this.fileDao = fileDao;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel loadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);

        FlightMap workingMap = context.getWorkingMap();

        // Lookup the file - on a recovery, we may have already created it, but not
        // finished. Or it might already exist, created by someone else.
        FSObjectBase fsObject = fileDao.retrieveByPathNoThrow(datasetId, loadModel.getTargetPath());
        if (fsObject == null) {
            // Nothing exists - create a new file
            FSFile newFile = new FSFile()
                .mimeType(loadModel.getMimeType())
                .flightId(context.getFlightId())
                .datasetId(dataset.getId())
                .objectType(FSObjectType.INGESTING_FILE)
                .path(loadModel.getTargetPath())
                .description(loadModel.getDescription());

            UUID objectId = fileDao.createFileStart(newFile);
            workingMap.put(FileMapKeys.OBJECT_ID, objectId.toString());

            return StepResult.getStepResultSuccess();
        }

        // OK, something exists. If it is a file and this flight created it, then we record it
        // and claim success. Otherwise someone else created it and we throw.
        if (!(fsObject instanceof FSFile)) {
            throw new FileSystemCorruptException("This should be a file!");
        }
        FSFile fsFile = (FSFile)fsObject;
        if (StringUtils.equals(fsFile.getFlightId(), context.getFlightId())) {
            workingMap.put(FileMapKeys.OBJECT_ID, fsFile.getObjectId().toString());
            return StepResult.getStepResultSuccess();
        }
        throw new FileSystemObjectAlreadyExistsException("Path already exists: " + fsFile.getPath());
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel loadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        fileDao.createFileStartUndo(datasetId, loadModel.getTargetPath(), context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

}
