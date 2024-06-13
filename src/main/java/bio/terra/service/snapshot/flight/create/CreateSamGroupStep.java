package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateSamGroupStep implements Step {
  private final IamService iamClient;
  private final SnapshotRequestModel snapshotReq;

  public CreateSamGroupStep(IamService iamClient, SnapshotRequestModel snapshotReq) {
    this.iamClient = iamClient;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    context
        .getWorkingMap()
        .put(SnapshotWorkingMapKeys.SAM_GROUP, iamClient.createGroup(snapshotReq.getName()));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String createdGroup =
        context.getWorkingMap().get(SnapshotWorkingMapKeys.SAM_GROUP, String.class);
    if (createdGroup != null) {
      iamClient.deleteGroup(createdGroup);
    }
    return StepResult.getStepResultSuccess();
  }
}
