package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FileDao;
import bio.terra.flight.file.FileMapKeys;
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
    private final FileDao fileDao;
    private final FileService fileService;
    private final GcsPdao gcsPdao;
    private final Study study;

    public IngestFilePrimaryDataStep(FileDao fileDao, Study study, FileService fileService, GcsPdao gcsPdao) {
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

        FSObject fsObject = fileDao.retrieve(objectId);
        fsObject = gcsPdao.copyFile(study, fileLoadModel, fsObject);

        workingMap.put(FileMapKeys.CHECKSUM_MD5, fsObject.getChecksumMd5());
        workingMap.put(FileMapKeys.CHECKSUM_CRC32C, fsObject.getChecksumCrc32c());
        workingMap.put(FileMapKeys.GSPATH, fsObject.getGspath());
        workingMap.put(FileMapKeys.SIZE, fsObject.getSize());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO: delete the GSPATH
        return StepResult.getStepResultSuccess();
    }

}
