package bio.terra.service.filedata.flight.delete;

import bio.terra.common.BaseStep;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

public class DeleteFileDirectoryStep extends BaseStep {
  private final FireStoreDao fileDao;
  private final String fileId;
  private final Dataset dataset;

  public DeleteFileDirectoryStep(FireStoreDao fileDao, String fileId, Dataset dataset) {
    this.fileDao = fileDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult perform() throws InterruptedException {
    try {
      boolean found = fileDao.deleteDirectoryEntry(dataset, fileId);
      DeleteResponseModel.ObjectStateEnum stateEnum =
          (found)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
      DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
      setResponse(deleteResponseModel, HttpStatus.OK);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }
}
