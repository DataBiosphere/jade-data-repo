package bio.terra.service.snapshot.flight.duos;

import static bio.terra.service.snapshot.flight.duos.SnapshotDuosFlightUtils.getFirecloudGroup;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddDuosFirecloudReaderStep implements Step {

  private final IamService iamService;
  private final AuthenticatedUserRequest userReq;
  private final UUID snapshotId;

  public AddDuosFirecloudReaderStep(
      IamService iamService, AuthenticatedUserRequest userReq, UUID snapshotId) {
    this.iamService = iamService;
    this.userReq = userReq;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    iamService.addPolicyMember(
        userReq,
        IamResourceType.DATASNAPSHOT,
        snapshotId,
        IamRole.READER.toString(),
        getFirecloudGroup(context).getFirecloudGroupEmail());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    iamService.deletePolicyMember(
        userReq,
        IamResourceType.DATASNAPSHOT,
        snapshotId,
        IamRole.READER.toString(),
        getFirecloudGroup(context).getFirecloudGroupEmail());
    return StepResult.getStepResultSuccess();
  }
}
