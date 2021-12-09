package bio.terra.service.resourcemanagement.azure;

import java.util.Objects;
import java.util.UUID;

public class AzureAuthorizedCacheKey {
  // purpose of this object is to contain 4 objects
  private final UUID subscriptionId;
  private final String resourceGroupName;
  private final String storageAccountResourceName;

  public AzureAuthorizedCacheKey(
      UUID subscriptionId, String resourceGroupName, String storageAccountResourceName) {
    this.subscriptionId = subscriptionId;
    this.resourceGroupName = resourceGroupName;
    this.storageAccountResourceName = storageAccountResourceName;
  }

  public UUID getSubscriptionId() {
    return subscriptionId;
  }

  public String getResourceGroupName() {
    return resourceGroupName;
  }

  public String getStorageAccountResourceName() {
    return storageAccountResourceName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AzureAuthorizedCacheKey)) {
      return false;
    }
    AzureAuthorizedCacheKey that = (AzureAuthorizedCacheKey) o;
    return Objects.equals(getSubscriptionId(), that.getSubscriptionId())
        && Objects.equals(getResourceGroupName(), that.getResourceGroupName())
        && Objects.equals(getStorageAccountResourceName(), that.getStorageAccountResourceName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getSubscriptionId(), getResourceGroupName(), getStorageAccountResourceName());
  }
}
