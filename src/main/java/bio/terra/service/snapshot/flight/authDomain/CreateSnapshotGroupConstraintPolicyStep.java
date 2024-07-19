package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoUpdateRequest;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotGroupConstraintPolicyStep implements Step {

  private final PolicyService policyService;
  private final UUID snapshotId;

  public CreateSnapshotGroupConstraintPolicyStep(PolicyService policyService, UUID snapshotId) {
    this.policyService = policyService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    policyService.createOrUpdatePao(
        snapshotId, TpsObjectType.SNAPSHOT, getTpsPolicyInputs(flightContext));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    TpsPaoUpdateRequest removeRequest =
        new TpsPaoUpdateRequest().removeAttributes(getTpsPolicyInputs(flightContext));
    policyService.updatePao(removeRequest, snapshotId);
    return StepResult.getStepResultSuccess();
  }

  private TpsPolicyInputs getTpsPolicyInputs(FlightContext flightContext) {
    List<String> userGroups =
        flightContext
            .getWorkingMap()
            .get(
                SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS,
                new TypeReference<>() {});
    return new TpsPolicyInputs()
        .inputs(List.of(PolicyService.getGroupConstraintPolicyInput(userGroups)));
  }
}
