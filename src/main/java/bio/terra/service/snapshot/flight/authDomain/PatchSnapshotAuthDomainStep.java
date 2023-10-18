package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.exception.SnapshotAuthDomainExistsException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class PatchSnapshotAuthDomainStep extends DefaultUndoStep {

  private final IamService iamService;

  private final AuthenticatedUserRequest userRequest;
  private final UUID snapshotId;

  private final List<String> userGroups;

  public PatchSnapshotAuthDomainStep(
      IamService iamService,
      AuthenticatedUserRequest userRequest,
      UUID snapshotId,
      List<String> userGroups) {
    this.iamService = iamService;
    this.userRequest = userRequest;
    this.snapshotId = snapshotId;
    this.userGroups = userGroups;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<String> existingAuthDomain =
        iamService.retrieveAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId);
        if (!existingAuthDomain.isEmpty()) {
          return new StepResult(
              StepStatus.STEP_RESULT_FAILURE_FATAL,
              new SnapshotAuthDomainExistsException(
                  "Snapshot " + snapshotId + " already has an auth domain set: " +
                      existingAuthDomain));
        }
    iamService.patchAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId, userGroups);
    return StepResult.getStepResultSuccess();
  }
}
