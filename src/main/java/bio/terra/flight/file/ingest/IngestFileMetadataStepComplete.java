package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FileDao;
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
        String checksumMd5 = workingMap.get(FileMapKeys.CHECKSUM_MD5, String.class);
        String checksumCrc32c = workingMap.get(FileMapKeys.CHECKSUM_CRC32C, String.class);
        String gspath = workingMap.get(FileMapKeys.GSPATH, String.class);
        Long size = workingMap.get(FileMapKeys.SIZE, Long.class);

        FSObject fsObject = fileDao.retrieveFile(objectId);
        fsObject
            .checksumMd5(checksumMd5)
            .checksumCrc32c(checksumCrc32c)
            .size(size)
            .gspath(gspath)
            .flightId(context.getFlightId());

        fileDao.createFileComplete(fsObject);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSObject(fsObject));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        FSObject fsObject = fileDao.retrieveFile(objectId);
        fsObject.flightId(context.getFlightId());
        fileDao.createFileCompleteUndo(fsObject);
        return StepResult.getStepResultSuccess();
    }

}
