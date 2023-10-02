package bio.terra.service.dataset.flight.upgrade.disableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyServiceApiException;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisableSecureMonitoringDeleteSourceDatasetAndSnapshotsTpsPolicyStep
    extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(
          DisableSecureMonitoringDeleteSourceDatasetAndSnapshotsTpsPolicyStep.class);

  private final PolicyService policyService;
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userRequest;

  public DisableSecureMonitoringDeleteSourceDatasetAndSnapshotsTpsPolicyStep(
      SnapshotService snapshotService,
      PolicyService policyService,
      AuthenticatedUserRequest userRequest) {
    this.snapshotService = snapshotService;
    this.policyService = policyService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    List<UUID> snapshotsToDeletePolicies =
        snapshotService.enumerateSnapshotIdsForDataset(datasetId, userRequest);

    snapshotsToDeletePolicies.forEach(
        snapshotId -> {
          try {
            policyService.deletePaoIfExists(snapshotId);
            logger.info("Policy access object deleted for snapshot {}", snapshotId);
          } catch (PolicyServiceApiException ex) {
            logger.warn("Error deleting policy object for snapshot {}", snapshotId);
          }
        });
    return StepResult.getStepResultSuccess();
  }
}
