package bio.terra.service.snapshot.flight.setpublic;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class SetSnapshotPublicStep implements Step {
  private final UUID snapshotId;
  private final boolean setPublic;
  private final AuthenticatedUserRequest userReq;
  private final IamService iamService;

  public SetSnapshotPublicStep(
      UUID snapshotId, boolean setPublic, AuthenticatedUserRequest userReq, IamService iamService) {
    this.snapshotId = snapshotId;
    this.setPublic = setPublic;
    this.userReq = userReq;
    this.iamService = iamService;
  }

  private StepResult setPublic(boolean publicVal) {
    try {
      iamService.setPolicyPublicV2(
          userReq.getToken(),
          IamResourceType.DATASNAPSHOT,
          snapshotId,
          IamRole.READER.name(),
          publicVal);
      return StepResult.getStepResultSuccess();
    } catch (Exception e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    return setPublic(setPublic);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return setPublic(!setPublic);
  }
}
