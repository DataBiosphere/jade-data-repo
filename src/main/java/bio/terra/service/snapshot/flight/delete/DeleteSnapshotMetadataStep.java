package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.BaseStep;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.http.HttpStatus;

public class DeleteSnapshotMetadataStep extends BaseStep {

  private final SnapshotDao snapshotDao;
  private final UUID snapshotId;

  public DeleteSnapshotMetadataStep(SnapshotDao snapshotDao, UUID snapshotId) {
    this.snapshotDao = snapshotDao;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult perform() {
    DeleteResponseModel.ObjectStateEnum stateEnum;
    try {
      stateEnum =
          snapshotDao.delete(snapshotId)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    } catch (SnapshotNotFoundException ex) {
      stateEnum = DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    } catch (CannotSerializeTransactionException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }

    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    setResponse(deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undo() {
    // This step is not undoable. We only get here when the
    // do method has a dismal failure.
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
