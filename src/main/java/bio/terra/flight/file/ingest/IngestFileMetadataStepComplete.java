package bio.terra.flight.file.ingest;

import bio.terra.dao.FileDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSObject;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class IngestFileMetadataStepComplete implements Step {
    private final FileDao fileDao;
    private final FileService fileService;

    public IngestFileMetadataStepComplete(FileDao fileDao, FileService fileService) {
        this.fileDao = fileDao;
        this.fileService = fileService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        String checksum = workingMap.get(FileMapKeys.CHECKSUM, String.class);
        String gspath = workingMap.get(FileMapKeys.GSPATH, String.class);
        Long size = workingMap.get(FileMapKeys.SIZE, Long.class);

        FSObject fsObject = fileDao.retrieveFile(objectId);
        fsObject
            .checksum(checksum)
            .size(size)
            .gspath(gspath)
            .creatingFlightId(context.getFlightId());

        fileDao.createFileComplete(fsObject);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSObject(fsObject));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        FSObject fsObject = fileDao.retrieveFile(objectId);
        fsObject.creatingFlightId(context.getFlightId());
        fileDao.createFileCompleteUndo(fsObject);
        return StepResult.getStepResultSuccess();
    }

}
