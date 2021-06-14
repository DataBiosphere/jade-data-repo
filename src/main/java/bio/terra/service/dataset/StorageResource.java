package bio.terra.service.dataset;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;
import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "cloudPlatform")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AzureStorageResource.class, name = "azure"),
    @JsonSubTypes.Type(value = GoogleStorageResource.class, name = "gcp")
})
public abstract class StorageResource<Region extends CloudRegion, Resource extends CloudResource> {

    private final UUID datasetId;
    private final Resource cloudResource;
    private final Region cloudRegion;

    public StorageResource(UUID datasetId, Resource cloudResource, Region cloudRegion) {
        this.datasetId = datasetId;
        this.cloudResource = cloudResource;
        this.cloudRegion = cloudRegion;
    }

    UUID getDatasetId() {
        return datasetId;
    }

    public abstract CloudPlatform getCloudPlatform();

    public Resource getCloudResource() {
        return cloudResource;
    }

    public Region getRegion() {
        return cloudRegion;
    }

    public StorageResourceModel toModel() {
        return new StorageResourceModel()
            .cloudPlatform(getCloudPlatform())
            .cloudResource(getCloudResource().getValue())
            .region(getRegion().getValue());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (StorageResource<?, ?>) o;
        return Objects.equals(getDatasetId(), that.getDatasetId()) &&
            getCloudPlatform() == that.getCloudPlatform() &&
            getCloudResource() == that.getCloudResource() &&
            getRegion() == that.getRegion();
    }

    public int hashCode() {
        return Objects.hash(getDatasetId(), getCloudPlatform(), getCloudResource(), getRegion());
    }

    public String toString() {
        return "StorageResource{" +
            "datasetId=" + getDatasetId() +
            ", cloudPlatform=" + getCloudPlatform() +
            ", cloudResource=" + getCloudResource() +
            ", region=" + getRegion() +
            '}';
    }
}
