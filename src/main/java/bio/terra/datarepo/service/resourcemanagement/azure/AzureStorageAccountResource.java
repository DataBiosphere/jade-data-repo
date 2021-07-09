package bio.terra.datarepo.service.resourcemanagement.azure;

import bio.terra.datarepo.app.model.AzureRegion;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class AzureStorageAccountResource {
  private UUID resourceId;
  private String flightId;
  private UUID profileId;
  private AzureApplicationDeploymentResource applicationResource;
  private String name;
  private String dataContainer;
  private String metadataContainer;
  private String dbName;
  private AzureRegion region;

  public AzureStorageAccountResource() {}

  public UUID getResourceId() {
    return resourceId;
  }

  public AzureStorageAccountResource resourceId(UUID resourceId) {
    this.resourceId = resourceId;
    return this;
  }

  public String getFlightId() {
    return flightId;
  }

  public AzureStorageAccountResource flightId(String flightId) {
    this.flightId = flightId;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public AzureStorageAccountResource profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public AzureApplicationDeploymentResource getApplicationResource() {
    return applicationResource;
  }

  public AzureStorageAccountResource applicationResource(
      AzureApplicationDeploymentResource applicationResource) {
    this.applicationResource = applicationResource;
    return this;
  }

  public String getName() {
    return name;
  }

  public AzureStorageAccountResource name(String name) {
    this.name = name;
    return this;
  }

  public String getDataContainer() {
    return dataContainer;
  }

  public AzureStorageAccountResource dataContainer(String dataContainer) {
    this.dataContainer = dataContainer;
    return this;
  }

  public String getMetadataContainer() {
    return metadataContainer;
  }

  public AzureStorageAccountResource metadataContainer(String metadataContainer) {
    this.metadataContainer = metadataContainer;
    return this;
  }

  public String getDbName() {
    return dbName;
  }

  public AzureStorageAccountResource dbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public AzureRegion getRegion() {
    return region;
  }

  public AzureStorageAccountResource region(AzureRegion region) {
    this.region = region;
    return this;
  }

  public String determineContainer(ContainerType containerType) {
    return containerType.getContainer(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AzureStorageAccountResource that = (AzureStorageAccountResource) o;
    return Objects.equals(resourceId, that.resourceId)
        && Objects.equals(flightId, that.flightId)
        && Objects.equals(profileId, that.profileId)
        && Objects.equals(applicationResource, that.applicationResource)
        && Objects.equals(name, that.name)
        && Objects.equals(dataContainer, that.dataContainer)
        && Objects.equals(metadataContainer, that.metadataContainer)
        && Objects.equals(dbName, that.dbName)
        && region == that.region;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        resourceId,
        flightId,
        profileId,
        applicationResource,
        name,
        dataContainer,
        metadataContainer,
        dbName,
        region);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("resourceId", resourceId)
        .append("flightId", flightId)
        .append("profileId", profileId)
        .append("applicationResource", applicationResource)
        .append("name", name)
        .append("dataContainer", dataContainer)
        .append("metadataContainer", metadataContainer)
        .append("dbName", dbName)
        .append("region", region)
        .toString();
  }

  public enum ContainerType {
    DATA() {
      String getContainer(AzureStorageAccountResource account) {
        return account.getDataContainer();
      }
    },
    METADATA() {
      String getContainer(AzureStorageAccountResource account) {
        return account.getMetadataContainer();
      }
    };

    abstract String getContainer(AzureStorageAccountResource account);
  }
}
