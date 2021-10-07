package bio.terra.service.snapshot.flight.create;

import bio.terra.common.CreateAzureStorageAccountStep;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;

public class CreateSnapshotCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {

  private ResourceService resourceService;

  public CreateSnapshotCreateAzureStorageAccountStep(
      DatasetService datasetService, ResourceService resourceService) {
    super(datasetService, resourceService);
    this.resourceService = resourceService;
  }

  public AzureStorageAccountResource getOrCreateStorageAccount(
      Dataset dataset, BillingProfileModel billingProfile, String flightId)
      throws InterruptedException {
    // TODO - replace w/ changes from trevyn's PR
    return resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
