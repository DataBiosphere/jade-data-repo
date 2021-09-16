package bio.terra.service.resourcemanagement.azure;

import java.util.UUID;

public class AzureStorageAuthInfo {
  private UUID subscriptionId;
  private String resourceGroupName;
  private String storageAccountResourceName;

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
