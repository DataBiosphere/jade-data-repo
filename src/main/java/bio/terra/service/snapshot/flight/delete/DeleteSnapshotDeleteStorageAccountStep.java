package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.function.Predicate;
import org.elasticsearch.ResourceNotFoundException;

public class DeleteSnapshotDeleteStorageAccountStep extends OptionalStep {

  private final UUID snapshotId;
  private final ResourceService resourceService;
  private final AzureStorageAccountService storageAccountService;

  public DeleteSnapshotDeleteStorageAccountStep(
      UUID snapshotId,
      ResourceService resourceService,
      AzureStorageAccountService storageAccountService,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.snapshotId = snapshotId;
    this.resourceService = resourceService;
    this.storageAccountService = storageAccountService;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    try {
      AzureStorageAccountResource snapshotStorageAccountResource =
          resourceService
              .getSnapshotStorageAccount(snapshotId)
              .orElseThrow(
                  () -> new ResourceNotFoundException("Snapshot storage account not found"));

      workingMap.put(
          SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME,
          snapshotStorageAccountResource.getName());

      storageAccountService.deleteCloudStorageAccount(snapshotStorageAccountResource);

    } catch (ResourceNotFoundException nfe) {
      // If we do not find the storage account, we assume things are already clean
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
