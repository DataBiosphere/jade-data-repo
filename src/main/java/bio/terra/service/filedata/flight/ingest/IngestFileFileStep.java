package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class IngestFileFileStep implements Step {
  private final FireStoreDao fileDao;
  private final FileService fileService;
  private final Dataset dataset;

  public IngestFileFileStep(FireStoreDao fileDao, FileService fileService, Dataset dataset) {
    this.fileDao = fileDao;
    this.fileService = fileService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      FlightMap inputParameters = context.getInputParameters();
      FileLoadModel fileLoadModel =
          inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

      FSFileInfo fsFileInfo = workingMap.get(FileMapKeys.FILE_INFO, FSFileInfo.class);
      String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

      FireStoreFile newFile =
          new FireStoreFile()
              .fileId(fileId)
              .mimeType(fileLoadModel.getMimeType())
              .description(fileLoadModel.getDescription())
              .bucketResourceId(fsFileInfo.getBucketResourceId())
              .fileCreatedDate(fsFileInfo.getCreatedDate())
              .gspath(fsFileInfo.getCloudPath())
              .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
              .checksumMd5(fsFileInfo.getChecksumMd5())
              .userSpecifiedMd5(fsFileInfo.isUserSpecifiedMd5())
              .size(fsFileInfo.getSize())
              .loadTag(fileLoadModel.getLoadTag());

      try {
        fileDao.upsertFileMetadata(dataset, newFile);
        // Retrieve to build the complete FSItem
        FSItem fsItem = fileDao.retrieveById(dataset, fileId, 1);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSItem(fsItem));
      } catch (FileSystemAbortTransactionException rex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
      }
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String itemId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    try {
      fileDao.deleteFileMetadata(dataset, itemId);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }
}
