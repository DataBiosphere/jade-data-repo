package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.http.HttpStatus;

public class DeleteSnapshotMetadataStep implements Step {

  private final SnapshotDao snapshotDao;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest userReq;

  public DeleteSnapshotMetadataStep(
      SnapshotDao snapshotDao, UUID snapshotId, AuthenticatedUserRequest userReq) {
    this.snapshotDao = snapshotDao;
    this.snapshotId = snapshotId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    DeleteResponseModel.ObjectStateEnum stateEnum;
    try {
      stateEnum =
          snapshotDao.delete(snapshotId, userReq)
              ? DeleteResponseModel.ObjectStateEnum.DELETED
              : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    } catch (SnapshotNotFoundException ex) {
      stateEnum = DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    } catch (CannotSerializeTransactionException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }

    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // This step is not undoable. We only get here when the
    // do method has a dismal failure.
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
