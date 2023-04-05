package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class DeleteSnapshotMetadataAzureStep implements Step {

  private final AzureStorageAccountService storageAccountService;

  public DeleteSnapshotMetadataAzureStep(AzureStorageAccountService storageAccountService) {
    this.storageAccountService = storageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String storageAccountResourceName =
        workingMap.get(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME, String.class);
    String storageAccountResourceTopLevelContainer =
        workingMap.get(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_TLC, String.class);

    storageAccountService.deleteCloudStorageAccountMetadata(
        storageAccountResourceName, storageAccountResourceTopLevelContainer, context.getFlightId());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
