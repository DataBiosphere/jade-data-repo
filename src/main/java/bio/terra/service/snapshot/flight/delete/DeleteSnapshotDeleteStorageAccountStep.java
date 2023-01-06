package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class DeleteSnapshotDeleteStorageAccountStep extends DefaultUndoStep {

  private final UUID snapshotId;
  private final ResourceService resourceService;
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
    FlightMap workingMap = context.getWorkingMap();

    AzureStorageAccountResource snapshotStorageAccountResource =
        resourceService.getSnapshotStorageAccount(snapshotId).orElse(null);

    // If we do not find the storage account, we assume things are already clean
    if (snapshotStorageAccountResource == null) {
      // Setting this so that subsequent step that uses the resource name does not run
      workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, false);
      return StepResult.getStepResultSuccess();
    }

    workingMap.put(
        SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME,
        snapshotStorageAccountResource.getName());

    storageAccountService.deleteCloudStorageAccount(snapshotStorageAccountResource);

    return StepResult.getStepResultSuccess();
  }
}
