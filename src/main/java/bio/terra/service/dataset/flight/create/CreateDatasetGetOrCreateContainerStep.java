package bio.terra.service.dataset.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.common.CreateAzureContainerStep;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import java.util.UUID;

public class CreateDatasetGetOrCreateContainerStep extends CreateAzureContainerStep {
  private final DatasetRequestModel datasetRequestModel;

  public CreateDatasetGetOrCreateContainerStep(
      ResourceService resourceService,
      DatasetRequestModel datasetRequestModel,
      AzureContainerPdao azureContainerPdao) {
    super(resourceService, azureContainerPdao);
    this.datasetRequestModel = datasetRequestModel;
  }

  @Override
  protected String getStorageAccountContextKey() {
    return CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE;
  }

  @Override
  protected AzureStorageAccountResource getAzureStorageAccountResource(
      FlightContext context, BillingProfileModel profileModel) throws InterruptedException {
    UUID datasetId = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

    return resourceService.getOrCreateDatasetStorageAccount(
        DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel, datasetId),
        profileModel,
        context.getFlightId());
  }
}
