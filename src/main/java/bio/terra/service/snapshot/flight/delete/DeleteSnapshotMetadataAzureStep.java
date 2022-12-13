package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;

public class DeleteSnapshotMetadataAzureStep extends DefaultUndoStep {

  private final AzureStorageAccountService storageAccountService;

  public DeleteSnapshotMetadataAzureStep(AzureStorageAccountService storageAccountService) {
    this.storageAccountService = storageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String storageAccountResourceName =
        workingMap.get(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME, String.class);

    storageAccountService.deleteCloudStorageAccountMetadata(
        storageAccountResourceName, context.getFlightId());

    return StepResult.getStepResultSuccess();
  }
}
