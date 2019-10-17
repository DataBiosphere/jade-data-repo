package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.model.FileLoadModel;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobMapKeys;
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
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

        FireStoreFile newFile = new FireStoreFile()
            .fileId(fileId)
            .mimeType(fileLoadModel.getMimeType())
            .description(fileLoadModel.getDescription())
            .bucketResourceId(fsFileInfo.getBucketResourceId())
            .fileCreatedDate(fsFileInfo.getCreatedDate())
            .gspath(fsFileInfo.getGspath())
            .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
            .checksumMd5(fsFileInfo.getChecksumMd5())
            .size(fsFileInfo.getSize());
        fileDao.createFileMetadata(dataset, newFile);

        // Retrieve to build the complete FSItem
        FSItem fsItem = fileDao.retrieveById(dataset, fileId, 1, true);

        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSItem(fsItem));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String itemId = workingMap.get(FileMapKeys.FILE_ID, String.class);
        fileDao.deleteFileMetadata(dataset, itemId);
        return StepResult.getStepResultSuccess();
    }

}
