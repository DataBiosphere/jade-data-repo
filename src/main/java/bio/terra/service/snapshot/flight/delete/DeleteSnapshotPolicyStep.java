package bio.terra.service.snapshot.flight.delete;

import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotPolicyStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(DeleteSnapshotPolicyStep.class);
  private final DatasetService datasetService;
  private final PolicyService policyService;
  private final UUID snapshotId;

  public DeleteSnapshotPolicyStep(
      DatasetService datasetService, PolicyService policyService, UUID snapshotId) {
    this.datasetService = datasetService;
    this.policyService = policyService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap map = flightContext.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    if (dataset.isSecureMonitoringEnabled()) {
      policyService.deletePaoIfExists(snapshotId);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap map = flightContext.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    if (dataset.isSecureMonitoringEnabled()) {
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
}
