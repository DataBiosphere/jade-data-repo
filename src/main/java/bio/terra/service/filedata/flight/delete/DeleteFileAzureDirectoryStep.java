package bio.terra.service.filedata.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.*;
import org.springframework.http.HttpStatus;

public class DeleteFileAzureDirectoryStep implements Step {
  private final TableDao tableDao;
  private final String fileId;
  private final Dataset dataset;

  public DeleteFileAzureDirectoryStep(TableDao tableDao, String fileId, Dataset dataset) {
    this.tableDao = tableDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccountResource =
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);
    try {
      boolean found =
          tableDao.deleteDirectoryEntry(fileId, billingProfileModel, storageAccountResource);
      DeleteResponseModel.ObjectStateEnum state =
          (found)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
      DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(state);
      FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // No undo is possible
    return StepResult.getStepResultSuccess();
  }
}
