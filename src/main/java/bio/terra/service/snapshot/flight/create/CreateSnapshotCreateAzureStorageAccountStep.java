package bio.terra.service.snapshot.flight.create;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateSnapshotCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {

  public CreateSnapshotCreateAzureStorageAccountStep(
      DatasetService datasetService, ResourceService resourceService) {
    super(datasetService, resourceService);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    getOrCreateDatasetStorageAccount(context);
    getOrCreateSnapshotStorageAccount(context);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
