package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record AdjustStewardMembersStep(
    AuthenticatedUserRequest userReq, IamService iamService, boolean inheritSteward)
    implements Step {

  interface AddRemoveApi {
    void addRemoveMember(
        AuthenticatedUserRequest userReq,
        IamResourceType iamResourceType,
        UUID resourceId,
        IamRole policy,
        String userEmail);
  }

  private void addRemoveStewardMembers(FlightContext context, boolean inheritSteward) {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> snapshotIds =
        Objects.requireNonNull(workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class));
    FlightMap inputParams = context.getInputParameters();
    List<String> custodians =
        Objects.requireNonNull(
            inputParams.get(JobMapKeys.CUSTODIAN_USERS.getKeyName(), List.class));
    AddRemoveApi api =
        inheritSteward ? iamService::addPolicyMember : iamService::deletePolicyMember;
    for (var snapshotId : snapshotIds) {
      for (var email : custodians) {
        api.addRemoveMember(
            userReq, IamResourceType.DATASNAPSHOT, snapshotId, IamRole.CUSTODIAN, email);
      }
    }
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    addRemoveStewardMembers(context, inheritSteward);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    addRemoveStewardMembers(context, !inheritSteward);
    return StepResult.getStepResultSuccess();
  }
}
