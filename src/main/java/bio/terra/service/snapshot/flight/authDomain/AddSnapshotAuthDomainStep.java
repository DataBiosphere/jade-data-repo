package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamInternalServerErrorException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.exception.AuthDomainGroupNotFoundException;
import bio.terra.service.snapshot.exception.SnapshotAuthDomainExistsException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.UUID;

// Note: there is no undo step because Sam does not support removing user groups from an auth domain
public class AddSnapshotAuthDomainStep extends DefaultUndoStep {

  private final IamService iamService;

  private final AuthenticatedUserRequest userRequest;
  private final UUID snapshotId;

  public AddSnapshotAuthDomainStep(
      IamService iamService, AuthenticatedUserRequest userRequest, UUID snapshotId) {
    this.iamService = iamService;
    this.userRequest = userRequest;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<String> userGroups =
        context
            .getWorkingMap()
            .get(
                SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS,
                new TypeReference<>() {});
    List<String> existingAuthDomain =
        iamService.retrieveAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId);
    if (!existingAuthDomain.isEmpty()) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new SnapshotAuthDomainExistsException(
              "Snapshot " + snapshotId + " already has an auth domain set: " + existingAuthDomain));
    }
    try {
      iamService.patchAuthDomain(userRequest, IamResourceType.DATASNAPSHOT, snapshotId, userGroups);
    } catch (IamInternalServerErrorException e) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new AuthDomainGroupNotFoundException(
              "One or more of these groups do not exist: " + userGroups));
    }
    return StepResult.getStepResultSuccess();
  }
}
