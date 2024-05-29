package bio.terra.service.snapshot.flight.create;

import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.SnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CreateSnapshotPolicyStep extends SnapshotPolicyStep {

  private final boolean enableSecureMonitoring;
  private final UUID snapshotId;

  public CreateSnapshotPolicyStep(
      PolicyService policyService, boolean enableSecureMonitoring, UUID snapshotId) {
    super(policyService);
    this.enableSecureMonitoring = enableSecureMonitoring;
    this.snapshotId = snapshotId;
  }

  @Override
  public boolean isSecureMonitoringEnabled(FlightContext flightContext) {
    return enableSecureMonitoring;
  }

  @Override
  public UUID getSnapshotId(FlightContext flightContext) {
    return snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    return createPaoStep(flightContext);
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return deletePaoStep(flightContext);
  }
}
