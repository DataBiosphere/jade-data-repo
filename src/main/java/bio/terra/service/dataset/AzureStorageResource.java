package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;
import java.util.UUID;

@JsonTypeName("azure")
public class AzureStorageResource implements StorageResource<AzureRegion, AzureCloudResource> {

    private UUID datasetId;
    private AzureCloudResource cloudResource;
    private AzureRegion region;

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
        return CloudPlatform.AZURE;
    }

    @Override
    public AzureCloudResource getCloudResource() {
        return cloudResource;
    }

    @Override
    public AzureStorageResource cloudResource(AzureCloudResource cloudResource) {
        this.cloudResource = (AzureCloudResource) cloudResource;
        return this;
    }

    @Override
    public AzureRegion getRegion() {
        return region;
    }

    @Override
    public AzureStorageResource region(AzureRegion region) {
        this.region = (AzureRegion) region;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureStorageResource that = (AzureStorageResource) o;
        return Objects.equals(datasetId, that.datasetId) &&
            cloudResource == that.cloudResource &&
            region == that.region;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetId, cloudResource, region);
    }

    @Override
    public String toString() {
        return "AzureStorageResource{" +
            "datasetId=" + getDatasetId() +
            ", cloudPlatform=" + getCloudPlatform() +
            ", cloudResource=" + getCloudResource() +
            ", region=" + getRegion() +
            '}';
    }
}
