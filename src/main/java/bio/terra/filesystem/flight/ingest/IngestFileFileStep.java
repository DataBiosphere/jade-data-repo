package bio.terra.filesystem.flight.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreFile;
import bio.terra.filesystem.flight.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.model.FileLoadModel;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;


public class IngestFileFileStep implements Step {
    private final FireStoreDao fileDao;
    private final FileService fileService;
    private final Dataset dataset;

    public IngestFileFileStep(FireStoreDao fileDao,
                              FileService fileService,
                              Dataset dataset) {
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        FSFileInfo fsFileInfo = workingMap.get(FileMapKeys.FILE_INFO, FSFileInfo.class);
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);

        FireStoreFile newFile = new FireStoreFile()
            .objectId(objectId)
            .mimeType(fileLoadModel.getMimeType())
            .description(fileLoadModel.getDescription())
            .profileId(fileLoadModel.getProfileId())
            .region(fsFileInfo.getRegion())
            .bucketResourceId(fsFileInfo.getBucketResourceId())
            .fileCreatedDate(fsFileInfo.getCreatedDate())
            .gspath(fsFileInfo.getGspath())
            .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
            .checksumMd5(fsFileInfo.getChecksumMd5())
            .size(fsFileInfo.getSize());
        fileDao.createFileObject(dataset, newFile);

        // Retrieve to build the whole return object
        FSObjectBase fsObject = fileDao.retrieveById(dataset, objectId, true);

        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSObject(fsObject));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        fileDao.deleteFileObject(dataset, objectId);
        return StepResult.getStepResultSuccess();
    }

}
