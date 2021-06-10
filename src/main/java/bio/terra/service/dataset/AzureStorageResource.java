package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;
import java.util.UUID;

@JsonTypeName("azure")
public class AzureStorageResource implements StorageResource {

    private UUID datasetId;
    private CloudPlatform cloudPlatform;
    private AzureCloudResource cloudResource;
    private AzureRegion region;

    AzureStorageResource() {
        this.cloudPlatform = CloudPlatform.AZURE;
    }

    @Override
    public UUID getDatasetId() {
        return datasetId;
    }

    @Override
    public AzureStorageResource datasetId(UUID datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return cloudPlatform;
    }

    @Override
    public CloudResource getCloudResource() {
        return cloudResource;
    }

    @Override
    public AzureStorageResource cloudResource(CloudResource cloudResource) {
        this.cloudResource = (AzureCloudResource) cloudResource;
        return this;
    }

    @Override
    public CloudRegion getRegion() {
        return region;
    }

    @Override
    public AzureStorageResource region(CloudRegion region) {
        this.region = (AzureRegion) region;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureStorageResource that = (AzureStorageResource) o;
        return Objects.equals(datasetId, that.datasetId) &&
            cloudPlatform == that.cloudPlatform &&
            cloudResource == that.cloudResource &&
            region == that.region;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetId, cloudPlatform, cloudResource, region);
    }

    @Override
    public String toString() {
        return "StorageResource{" +
            "datasetId=" + datasetId +
            ", cloudPlatform=" + cloudPlatform +
            ", cloudResource=" + cloudResource +
            ", region=" + region +
            '}';
    }
}
