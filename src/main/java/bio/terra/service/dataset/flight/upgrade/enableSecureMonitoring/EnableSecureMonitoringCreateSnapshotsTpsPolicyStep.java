package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnableSecureMonitoringCreateSnapshotsTpsPolicyStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(EnableSecureMonitoringCreateSnapshotsTpsPolicyStep.class);

  private final PolicyService policyService;
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userRequest;

  public EnableSecureMonitoringCreateSnapshotsTpsPolicyStep(
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
    List<UUID> snapshotPoliciesToCreate =
        snapshotService.enumerateSnapshotIdsForDataset(datasetId, userRequest);
    TpsPolicyInput protectedDataPolicy = PolicyService.getProtectedDataPolicyInput();
    TpsPolicyInputs policyInputs = new TpsPolicyInputs().addInputsItem(protectedDataPolicy);

    snapshotPoliciesToCreate.forEach(
        snapshotId -> {
          try {
            policyService.createSnapshotPao(snapshotId, policyInputs);
            logger.info("Policy access object created for snapshot {}", snapshotId);
          } catch (PolicyConflictException | PolicyServiceDuplicateException ex) {
            logger.warn("Policy access object already exists for snapshot {}", snapshotId);
          }
        });
    return StepResult.getStepResultSuccess();
  }
}
