package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import java.util.UUID;

public record AzureStorageAuthInfo(
    UUID subscriptionId, String resourceGroupName, String storageAccountResourceName) {
  public static AzureStorageAuthInfo azureStorageAuthInfoBuilder(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    UUID subscriptionId = profileModel.getSubscriptionId();
    String resourceGroupName =
        storageAccountResource.getApplicationResource().getAzureResourceGroupName();
    String storageAccountResourceName = storageAccountResource.getName();
    return new AzureStorageAuthInfo(subscriptionId, resourceGroupName, storageAccountResourceName);
  }
}
