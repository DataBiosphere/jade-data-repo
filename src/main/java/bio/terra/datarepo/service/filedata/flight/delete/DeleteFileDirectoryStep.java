package bio.terra.datarepo.service.filedata.flight.delete;

import bio.terra.datarepo.common.FlightUtils;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.datarepo.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

public class DeleteFileDirectoryStep implements Step {
  private final FireStoreDao fileDao;
  private final String fileId;
  private final Dataset dataset;

  public DeleteFileDirectoryStep(FireStoreDao fileDao, String fileId, Dataset dataset) {
    this.fileDao = fileDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    try {
      boolean found = fileDao.deleteDirectoryEntry(dataset, fileId);
      DeleteResponseModel.ObjectStateEnum stateEnum =
          (found)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
      DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
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
