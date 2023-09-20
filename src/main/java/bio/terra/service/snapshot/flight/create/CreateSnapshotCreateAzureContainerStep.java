package bio.terra.service.snapshot.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.common.CreateAzureContainerStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;

public class CreateSnapshotCreateAzureContainerStep extends CreateAzureContainerStep {
  public CreateSnapshotCreateAzureContainerStep(
      ResourceService resourceService, AzureContainerPdao azureContainerPdao) {
    super(resourceService, azureContainerPdao, CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_RESOURCE);
  }

  @Override
  protected AzureStorageAccountResource getAzureStorageAccountResource(
      FlightContext context, BillingProfileModel profileModel) throws InterruptedException {
    return context.getWorkingMap().get(storageAccountContextKey, AzureStorageAccountResource.class);
  }
}
