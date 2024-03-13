package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.common.BaseStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AddAuthDomainResponseModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

public class AddSnapshotAuthDomainSetResponseStep extends BaseStep {

  private final IamService iamService;
  private final AuthenticatedUserRequest userRequest;
  private final UUID snapshotId;

  public AddSnapshotAuthDomainSetResponseStep(
      IamService iamService, AuthenticatedUserRequest userRequest, UUID snapshotId) {
    this.iamService = iamService;
    this.userRequest = userRequest;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult perform() {
    List<String> authDomain =
        iamService.retrieveAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId);
    setResponse(new AddAuthDomainResponseModel().authDomain(authDomain));
    return StepResult.getStepResultSuccess();
  }
}
