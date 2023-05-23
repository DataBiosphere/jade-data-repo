package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSnapshotPolicyStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(CreateSnapshotPolicyStep.class);

  private final PolicyService policyService;
  private final boolean enableSecureMonitoring;

  public CreateSnapshotPolicyStep(PolicyService policyService, boolean enableSecureMonitoring) {
    this.policyService = policyService;
    this.enableSecureMonitoring = enableSecureMonitoring;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    if (enableSecureMonitoring) {
      FlightMap flightMap = flightContext.getWorkingMap();
      UUID snapshotId = flightMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
      TpsPolicyInput protectedDataPolicy = PolicyService.getProtectedDataPolicyInput();
      TpsPolicyInputs policyInputs = new TpsPolicyInputs().addInputsItem(protectedDataPolicy);
      try {
        policyService.createSnapshotPao(snapshotId, policyInputs);
      } catch (PolicyConflictException ex) {
        logger.warn("Policy access object already exists for snapshot {}", snapshotId);
      } catch (FeatureNotImplementedException ex) {
        logger.info("Terra Policy Service is not enabled");
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    if (enableSecureMonitoring) {
      FlightMap flightMap = flightContext.getWorkingMap();
      UUID snapshotId = flightMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
      try {
        policyService.deletePaoIfExists(snapshotId);
      } catch (FeatureNotImplementedException ex) {
        logger.info("Terra Policy Service is not enabled");
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
