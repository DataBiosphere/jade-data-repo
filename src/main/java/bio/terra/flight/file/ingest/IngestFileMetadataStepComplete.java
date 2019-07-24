package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.Study;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;


public class IngestFileMetadataStepComplete implements Step {
    private final FireStoreFileDao fileDao;
    private final FileService fileService;
    private final  Study study;

    public IngestFileMetadataStepComplete(FireStoreFileDao fileDao,
                                          FileService fileService,
                                          Study study) {
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.study = study;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FSFileInfo fsFileInfo = workingMap.get(FileMapKeys.FILE_INFO, FSFileInfo.class);
        fsFileInfo.flightId(context.getFlightId());

        FSObjectBase fsObject = fileDao.createFileComplete(study, fsFileInfo);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSObject(fsObject));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        fileDao.createFileCompleteUndo(study, objectId);
        return StepResult.getStepResultSuccess();
    }

}
