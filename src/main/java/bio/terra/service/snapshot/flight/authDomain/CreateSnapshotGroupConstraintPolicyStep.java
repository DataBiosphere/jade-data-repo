package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoUpdateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotGroupConstraintPolicyStep implements Step {

  private final PolicyService policyService;
  private final UUID snapshotId;
  private final List<String> userGroups;

  public CreateSnapshotGroupConstraintPolicyStep(
      PolicyService policyService, UUID snapshotId, List<String> userGroups) {
    this.policyService = policyService;
    this.snapshotId = snapshotId;
    this.userGroups = userGroups;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    List<TpsPolicyInput> inputs =
        userGroups.stream().map(PolicyService::getGroupConstraintPolicyInput).toList();
    TpsPolicyInputs policyInputs = new TpsPolicyInputs().inputs(inputs);
    policyService.createOrUpdatePao(snapshotId, TpsObjectType.SNAPSHOT, policyInputs);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    List<TpsPolicyInput> inputs =
        userGroups.stream().map(PolicyService::getGroupConstraintPolicyInput).toList();
    TpsPolicyInputs policyInputs = new TpsPolicyInputs().inputs(inputs);
    TpsPaoUpdateRequest removeRequest = new TpsPaoUpdateRequest().removeAttributes(policyInputs);
    policyService.updatePao(removeRequest, snapshotId);
    return StepResult.getStepResultSuccess();
  }
}
