package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.function.Predicate;

public class DeleteSnapshotMetadataAzureStep extends OptionalStep {

  private final AzureStorageAccountService storageAccountService;

  public DeleteSnapshotMetadataAzureStep(
      AzureStorageAccountService storageAccountService, Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.storageAccountService = storageAccountService;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String storageAccountResourceName =
        workingMap.get(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME, String.class);

    storageAccountService.deleteCloudStorageAccountMetadata(
        storageAccountResourceName, context.getFlightId());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
