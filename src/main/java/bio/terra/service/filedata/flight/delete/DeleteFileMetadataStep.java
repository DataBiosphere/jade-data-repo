package bio.terra.service.filedata.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class DeleteFileMetadataStep implements Step {
  private final FireStoreDao fileDao;
  private final String fileId;
  private final Dataset dataset;

  public DeleteFileMetadataStep(FireStoreDao fileDao, String fileId, Dataset dataset) {
    this.fileDao = fileDao;
    this.fileId = fileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    try {
      fileDao.deleteFileMetadata(dataset, fileId);
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
