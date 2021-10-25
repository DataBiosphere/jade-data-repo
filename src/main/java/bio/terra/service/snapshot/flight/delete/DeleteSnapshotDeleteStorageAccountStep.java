package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.elasticsearch.ResourceNotFoundException;

public class DeleteSnapshotDeleteStorageAccountStep implements Step {

  private UUID snapshotId;
  private ResourceService resourceService;
  private final AzureStorageAccountService storageAccountService;

  public DeleteSnapshotDeleteStorageAccountStep(
      UUID snapshotId,
      ResourceService resourceService,
      AzureStorageAccountService storageAccountService) {
    this.snapshotId = snapshotId;
    this.resourceService = resourceService;
    this.storageAccountService = storageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {

    AzureStorageAccountResource snapshotStorageAccountResource =
        resourceService
            .getSnapshotStorageAccount(snapshotId)
            .orElseThrow(() -> new ResourceNotFoundException("Snapshot storage account not found"));

    storageAccountService.deleteCloudStorageAccount(
        snapshotStorageAccountResource, context.getFlightId());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
