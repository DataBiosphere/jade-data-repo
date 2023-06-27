package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import com.azure.resourcemanager.resources.fluentcore.arm.ResourceId;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class AzureApplicationDeploymentResource {
  private UUID id; // id of the project resource in the datarepo metadata
  private UUID profileId; // id of the associated billing profile
  private String azureApplicationDeploymentId; // azure's id of the application deployment
  private String azureApplicationDeploymentName; // azure's name of the application deployment
  private String azureResourceGroupName; // azure's name of the managed resource group
  private String azureSynapseWorkspaceName; // azure's name of the Synapse workspace
  private AzureRegion defaultRegion; // default region to create resources in
  private String storageAccountPrefix; // prefix to use to when creating a storage account
  private AzureStorageAccountSkuType
      storageAccountSkuType; // storage type to use when creating storage account

  // Default constructor for JSON serializer
  public AzureApplicationDeploymentResource() {}

  public UUID getId() {
    return id;
  }

  public AzureApplicationDeploymentResource id(UUID id) {
    this.id = id;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public AzureApplicationDeploymentResource profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public String getAzureApplicationDeploymentId() {
    return azureApplicationDeploymentId;
  }

  public AzureApplicationDeploymentResource azureApplicationDeploymentId(
      String azureApplicationDeploymentId) {
    this.azureApplicationDeploymentId = azureApplicationDeploymentId;
    return this;
  }

  public String getAzureApplicationDeploymentName() {
    return azureApplicationDeploymentName;
  }

  public AzureApplicationDeploymentResource azureApplicationDeploymentName(
      String azureApplicationDeploymentName) {
    this.azureApplicationDeploymentName = azureApplicationDeploymentName;
    return this;
  }

  public String getAzureResourceGroupName() {
    return azureResourceGroupName;
  }

  public AzureApplicationDeploymentResource azureResourceGroupName(String azureResourceGroupName) {
    this.azureResourceGroupName = azureResourceGroupName;
    return this;
  }

  public String getAzureSynapseWorkspaceName() {
    return azureSynapseWorkspaceName;
  }

  public AzureApplicationDeploymentResource azureSynapseWorkspaceName(
      String azureSynapseWorkspaceName) {
    this.azureSynapseWorkspaceName = azureSynapseWorkspaceName;
    return this;
  }

  public AzureRegion getDefaultRegion() {
    return defaultRegion;
  }

  public AzureApplicationDeploymentResource defaultRegion(AzureRegion defaultRegion) {
    this.defaultRegion = defaultRegion;
    return this;
  }

  public String getStorageAccountPrefix() {
    return storageAccountPrefix;
  }

  public AzureApplicationDeploymentResource storageAccountPrefix(String storageAccountPrefix) {
    this.storageAccountPrefix = storageAccountPrefix;
    return this;
  }

  public AzureStorageAccountSkuType getStorageAccountSkuType() {
    return storageAccountSkuType;
  }

  public AzureApplicationDeploymentResource storageAccountSkuType(
      AzureStorageAccountSkuType storageAccountSkuType) {
    this.storageAccountSkuType = storageAccountSkuType;
    return this;
  }

  public UUID getSubscriptionId() {
    return UUID.fromString(ResourceId.fromString(azureApplicationDeploymentId).subscriptionId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AzureApplicationDeploymentResource that = (AzureApplicationDeploymentResource) o;
    return Objects.equals(id, that.id)
        && Objects.equals(profileId, that.profileId)
        && Objects.equals(azureApplicationDeploymentId, that.azureApplicationDeploymentId)
        && Objects.equals(azureApplicationDeploymentName, that.azureApplicationDeploymentName)
        && Objects.equals(azureResourceGroupName, that.azureResourceGroupName)
        && Objects.equals(azureSynapseWorkspaceName, that.azureSynapseWorkspaceName)
        && defaultRegion == that.defaultRegion
        && Objects.equals(storageAccountPrefix, that.storageAccountPrefix)
        && Objects.equals(storageAccountSkuType, that.storageAccountSkuType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        profileId,
        azureApplicationDeploymentId,
        azureApplicationDeploymentName,
        azureResourceGroupName,
        azureSynapseWorkspaceName,
        defaultRegion,
        storageAccountPrefix,
        storageAccountSkuType);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("profileId", profileId)
        .append("azureApplicationDeploymentId", azureApplicationDeploymentId)
        .append("azureApplicationDeploymentName", azureApplicationDeploymentName)
        .append("azureResourceGroupName", azureResourceGroupName)
        .append("azureSynapseWorkspaceName", azureSynapseWorkspaceName)
        .append("defaultRegion", defaultRegion)
        .append("storageAccountPrefix", storageAccountPrefix)
        .append("storageAccountSkuType", storageAccountSkuType)
        .toString();
  }
}
