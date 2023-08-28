package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnableSecureMonitoringCreateTpsPolicyStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(EnableSecureMonitoringCreateTpsPolicyStep.class);

  private final UUID datasetId;
  private final PolicyService policyService;
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userRequest;

  public EnableSecureMonitoringCreateTpsPolicyStep(
      UUID datasetId,
      SnapshotService snapshotService,
      PolicyService policyService,
      AuthenticatedUserRequest userRequest) {
    this.datasetId = datasetId;
    this.snapshotService = snapshotService;
    this.policyService = policyService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    List<UUID> snapshotsToCreatePolicies = enumerateSnapshotIdsForDataset();
    TpsPolicyInput protectedDataPolicy = PolicyService.getProtectedDataPolicyInput();
    TpsPolicyInputs policyInputs = new TpsPolicyInputs().addInputsItem(protectedDataPolicy);

    snapshotsToCreatePolicies.forEach(
        snapshotId -> {
          try {
            policyService.createSnapshotPao(snapshotId, policyInputs);
          } catch (PolicyConflictException ex) {
            logger.warn("Policy access object already exists for snapshot {}", snapshotId);
          }
        });
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    List<UUID> snapshotsToCreatePolicies = enumerateSnapshotIdsForDataset();
    snapshotsToCreatePolicies.forEach(
        snapshotId -> {
          policyService.deletePaoIfExists(snapshotId);
        });
    return StepResult.getStepResultSuccess();
  }

  private List<UUID> enumerateSnapshotIdsForDataset() {
    return snapshotService
        .enumerateSnapshots(
            userRequest,
            0,
            Integer.MAX_VALUE,
            EnumerateSortByParam.NAME,
            SqlSortDirection.ASC,
            "",
            "",
            List.of(datasetId),
            List.of())
        .getItems()
        .stream()
        .map(snapshotSummaryModel -> snapshotSummaryModel.getId())
        .collect(Collectors.toList());
  }
}
