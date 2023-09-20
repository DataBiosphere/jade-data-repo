package bio.terra.service.snapshot.flight;

import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SnapshotPolicyStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotPolicyStep.class);

  private final PolicyService policyService;

  public SnapshotPolicyStep(PolicyService policyService) {
    this.policyService = policyService;
  }

  public abstract boolean isSecureMonitoringEnabled(FlightContext context)
      throws InterruptedException;

  public abstract UUID getSnapshotId(FlightContext context) throws InterruptedException;

  public StepResult createPaoStep(FlightContext flightContext) throws InterruptedException {
    if (isSecureMonitoringEnabled(flightContext)) {
      UUID snapshotId = getSnapshotId(flightContext);
      TpsPolicyInput protectedDataPolicy = PolicyService.getProtectedDataPolicyInput();
      TpsPolicyInputs policyInputs = new TpsPolicyInputs().addInputsItem(protectedDataPolicy);
      try {
        policyService.createSnapshotPao(snapshotId, policyInputs);
      } catch (PolicyConflictException ex) {
        logger.warn("Policy access object already exists for snapshot {}", snapshotId);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  public StepResult deletePaoStep(FlightContext flightContext) throws InterruptedException {
    if (isSecureMonitoringEnabled(flightContext)) {
      policyService.deletePaoIfExists(getSnapshotId(flightContext));
    }
    return StepResult.getStepResultSuccess();
  }
}
