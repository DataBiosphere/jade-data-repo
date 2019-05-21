package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObject;
import bio.terra.metadata.Study;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class IngestFilePrimaryDataStep implements Step {
    private final FireStoreFileDao fileDao;
    private final FileService fileService;
    private final GcsPdao gcsPdao;
    private final Study study;

    public IngestFilePrimaryDataStep(FireStoreFileDao fileDao, Study study, FileService fileService, GcsPdao gcsPdao) {
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.gcsPdao = gcsPdao;
        this.study = study;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));

        FSObject fsObject = fileDao.retrieve(study.getId(), objectId);
        FSFileInfo fsFileInfo = gcsPdao.copyFile(study, fileLoadModel, fsObject);
        workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        gcsPdao.deleteFile(study, objectId);
        return StepResult.getStepResultSuccess();
    }

}
