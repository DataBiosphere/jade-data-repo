package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import java.util.UUID;

public class AzureStorageAuthInfo {
  private UUID subscriptionId;
  private String resourceGroupName;
  private String storageAccountResourceName;

  public static AzureStorageAuthInfo azureStorageAuthInfoBuilder(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    return new AzureStorageAuthInfo()
        .subscriptionId(profileModel.getSubscriptionId())
        .resourceGroupName(
            storageAccountResource.getApplicationResource().getAzureResourceGroupName())
        .storageAccountResourceName(storageAccountResource.getName());
  }

  public AzureStorageAuthInfo() {}

  public UUID getSubscriptionId() {
    return subscriptionId;
  }

  public AzureStorageAuthInfo subscriptionId(UUID subscriptionId) {
    this.subscriptionId = subscriptionId;
    return this;
  }

  public String getResourceGroupName() {
    return resourceGroupName;
  }

  public AzureStorageAuthInfo resourceGroupName(String resourceGroupName) {
    this.resourceGroupName = resourceGroupName;
    return this;
  }

  public String getStorageAccountResourceName() {
    return storageAccountResourceName;
  }

  public AzureStorageAuthInfo storageAccountResourceName(String storageAccountResourceName) {
    this.storageAccountResourceName = storageAccountResourceName;
    return this;
  }
}
