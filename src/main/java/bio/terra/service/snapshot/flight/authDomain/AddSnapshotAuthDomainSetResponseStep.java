package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AddAuthDomainResponseModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class AddSnapshotAuthDomainSetResponseStep extends DefaultUndoStep {

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
  public StepResult doStep(FlightContext context) {
    List<String> authDomain =
        iamService.retrieveAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId);
    AddAuthDomainResponseModel response = new AddAuthDomainResponseModel().authDomain(authDomain);
    FlightUtils.setResponse(context, response, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }
}
