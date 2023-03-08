package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.CollectionType;
import bio.terra.common.FlightUtils;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.models.TableServiceException;

public class IngestFileAzureFileStep implements Step {
  private final TableDao tableDao;
  private final FileService fileService;
  private final Dataset dataset;

  public IngestFileAzureFileStep(TableDao tableDao, FileService fileService, Dataset dataset) {
    this.tableDao = tableDao;
    this.fileService = fileService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);

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
        tableDao.createFileMetadata(newFile, storageAuthInfo);
        // Retrieve to build the complete FSItem
        FSItem fsItem =
            tableDao.retrieveById(
                CollectionType.DATASET,
                dataset.getId(),
                fileId,
                1,
                storageAuthInfo,
                storageAuthInfo);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), fileService.fileModelFromFSItem(fsItem));
      } catch (TableServiceException rex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
      }
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String itemId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    AzureStorageAuthInfo storageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);

    try {
      tableDao.deleteFileMetadata(itemId, storageAuthInfo);
    } catch (TableServiceException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }
}
