package bio.terra.service.snapshot.flight.create;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;

public class CreateSnapshotCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {

  public CreateSnapshotCreateAzureStorageAccountStep(
      ResourceService resourceService, Dataset dataset) {
    super(resourceService, dataset);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    getOrCreateDatasetStorageAccount(context);
    getOrCreateSnapshotStorageAccount(context);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
