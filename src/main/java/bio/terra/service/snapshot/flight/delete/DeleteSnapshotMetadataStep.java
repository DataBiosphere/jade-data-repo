package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteSnapshotMetadataStep extends DefaultUndoStep {

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
    }

    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }
}
