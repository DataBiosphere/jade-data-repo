package bio.terra.service.filedata.flight.delete;

import bio.terra.common.BaseStep;
import bio.terra.common.StepInput;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.*;
import org.springframework.http.HttpStatus;

public class DeleteFileAzureDirectoryStep extends BaseStep {
  private final TableDao tableDao;
  private final String fileId;
  private final Dataset dataset;

  @StepInput AzureStorageAuthInfo datasetStorageAuthInfo;

  public DeleteFileAzureDirectoryStep(TableDao tableDao, String fileId, Dataset dataset) {
    this.tableDao = tableDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult perform() throws InterruptedException {
    try {
      boolean found =
          tableDao.deleteDirectoryEntry(
              fileId,
              datasetStorageAuthInfo,
              dataset.getId(),
              StorageTableName.DATASET.toTableName(dataset.getId()));
      DeleteResponseModel.ObjectStateEnum state =
          (found)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
      DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(state);
      setResponse(deleteResponseModel, HttpStatus.OK);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }
}
