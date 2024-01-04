package bio.terra.service.snapshot.flight.duos;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class RemoveDuosFirecloudReaderStep implements Step {

  private final IamService iamService;
  private final AuthenticatedUserRequest userReq;
  private final UUID snapshotId;
  private final DuosFirecloudGroupModel duosFirecloudGroupPrev;

  public RemoveDuosFirecloudReaderStep(
      IamService iamService,
      AuthenticatedUserRequest userReq,
      UUID snapshotId,
      DuosFirecloudGroupModel duosFirecloudGroupPrev) {
    this.iamService = iamService;
    this.userReq = userReq;
    this.snapshotId = snapshotId;
    this.duosFirecloudGroupPrev = duosFirecloudGroupPrev;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    iamService.deletePolicyMember(
        userReq,
        IamResourceType.DATASNAPSHOT,
        snapshotId,
        IamRole.READER.toString(),
        duosFirecloudGroupPrev.getFirecloudGroupEmail());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    iamService.addPolicyMember(
        userReq,
        IamResourceType.DATASNAPSHOT,
        snapshotId,
        IamRole.READER.toString(),
        duosFirecloudGroupPrev.getFirecloudGroupEmail());
    return StepResult.getStepResultSuccess();
  }
}
