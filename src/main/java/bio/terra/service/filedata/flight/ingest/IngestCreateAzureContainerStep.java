package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.common.CreateAzureContainerStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;

public class IngestCreateAzureContainerStep extends CreateAzureContainerStep {

  private final Dataset dataset;

  public IngestCreateAzureContainerStep(
      ResourceService resourceService, AzureContainerPdao azureContainerPdao, Dataset dataset) {
    super(resourceService, azureContainerPdao);
    this.dataset = dataset;
  }

  @Override
  protected String getStorageAccountContextKey() {
    return CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE;
  }

  @Override
  protected AzureStorageAccountResource getAzureStorageAccountResource(
      FlightContext context, BillingProfileModel profileModel) throws InterruptedException {
    return resourceService.getOrCreateDatasetStorageAccount(
        dataset, profileModel, context.getFlightId());
  }
}
