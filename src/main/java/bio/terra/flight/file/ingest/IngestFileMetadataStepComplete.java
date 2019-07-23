package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.Study;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class IngestFileMetadataStepComplete implements Step {
    private final FireStoreFileDao fileDao;
    private final FileService fileService;
    private final  StudyService studyService;

    public IngestFileMetadataStepComplete(FireStoreFileDao fileDao,
                                          FileService fileService,
                                          StudyService studyService) {
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.studyService = studyService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FSFileInfo fsFileInfo = workingMap.get(FileMapKeys.FILE_INFO, FSFileInfo.class);
        fsFileInfo.flightId(context.getFlightId());
        Study study = studyService.retrieve(UUID.fromString(fsFileInfo.getStudyId()));

        FSObjectBase fsObject = fileDao.createFileComplete(study, fsFileInfo);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSObject(fsObject));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        String studyId = inputParameters.get(JobMapKeys.STUDY_ID.getKeyName(), String.class);
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        Study study = studyService.retrieve(UUID.fromString(studyId));
        fileDao.createFileCompleteUndo(study, objectId);
        return StepResult.getStepResultSuccess();
    }

}
