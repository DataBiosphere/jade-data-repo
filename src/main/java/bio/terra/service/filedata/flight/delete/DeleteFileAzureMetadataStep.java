package bio.terra.service.filedata.flight.delete;

import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.*;

public class DeleteFileAzureMetadataStep implements Step {
  private final TableDao tableDao;
  private final String fileId;
  private final Dataset dataset;

  public DeleteFileAzureMetadataStep(TableDao tableDao, String fileId, Dataset dataset) {
    this.tableDao = tableDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        workingMap.get(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    try {
      tableDao.deleteFileMetadata(dataset.getId().toString(), fileId, storageAuthInfo);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // No possible undo
    return StepResult.getStepResultSuccess();
  }
}
