package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.SnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class DeleteSnapshotPolicyStep extends SnapshotPolicyStep {
  private final DatasetService datasetService;
  private final UUID snapshotId;

  public DeleteSnapshotPolicyStep(
      DatasetService datasetService, PolicyService policyService, UUID snapshotId) {
    super(policyService);
    this.datasetService = datasetService;
    this.snapshotId = snapshotId;
  }

  @Override
  public boolean isSecureMonitoringEnabled(FlightContext context) throws InterruptedException {
    FlightMap map = context.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    return dataset.isSecureMonitoringEnabled();
  }

  @Override
  public UUID getSnapshotId(FlightContext flightContext) {
    return snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    return deletePaoStep(flightContext);
  }

  @Override
  public StepResult undoStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    return createPaoStep(flightContext);
  }
}
