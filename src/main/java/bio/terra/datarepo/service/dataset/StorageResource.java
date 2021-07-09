package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.app.model.CloudRegion;
import bio.terra.datarepo.app.model.CloudResource;
import bio.terra.datarepo.model.CloudPlatform;
import bio.terra.datarepo.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;
import java.util.UUID;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "cloudPlatform")
@JsonSubTypes({
  @JsonSubTypes.Type(value = AzureStorageResource.class, name = "azure"),
  @JsonSubTypes.Type(value = GoogleStorageResource.class, name = "gcp")
})
public abstract class StorageResource<Resource extends CloudResource, Region extends CloudRegion> {

  private final UUID datasetId;
  private final Resource cloudResource;
  private final Region region;

  public StorageResource(UUID datasetId, Resource cloudResource, Region region) {
    this.datasetId = datasetId;
    this.cloudResource = cloudResource;
    this.region = region;
  }

  public UUID getDatasetId() {
    return datasetId;
  }

  public abstract CloudPlatform getCloudPlatform();

  public Resource getCloudResource() {
    return cloudResource;
  }

  public Region getRegion() {
    return region;
  }

  public StorageResourceModel toModel() {
    return new StorageResourceModel()
        .cloudPlatform(getCloudPlatform())
        .cloudResource(getCloudResource().getValue())
        .region(getRegion().getValue());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageResource<?, ?> that = (StorageResource<?, ?>) o;
    return Objects.equals(datasetId, that.getDatasetId())
        && cloudResource == that.getCloudResource()
        && region == that.getRegion();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCloudPlatform(), getDatasetId(), getCloudResource(), getRegion());
  }

  @Override
  public String toString() {
    return "StorageResource{"
        + "datasetId="
        + getDatasetId()
        + ", cloudPlatform="
        + getCloudPlatform()
        + ", cloudResource="
        + getCloudResource()
        + ", region="
        + getRegion()
        + '}';
  }
}
