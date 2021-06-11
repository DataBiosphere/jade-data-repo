package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;

import java.util.Objects;
import java.util.UUID;

public class StorageResource {

    private UUID datasetId;
    private CloudPlatform cloudPlatform;
    private GoogleCloudResource cloudResource;
    private GoogleRegion region;

    public StorageResourceModel toModel() {
        GoogleRegion regionOverride = region;
        if (cloudResource == GoogleCloudResource.FIRESTORE) {
            regionOverride = region.getRegionOrFallbackFirestoreRegion();
        } else if (cloudResource == GoogleCloudResource.BUCKET) {
            regionOverride = region.getRegionOrFallbackBucketRegion();
        }
        return new StorageResourceModel()
            .cloudPlatform(cloudPlatform)
            .cloudResource(cloudResource.toString())
            .region(regionOverride.toString());
    }

    public UUID getDatasetId() {
        return datasetId;
    }

    public StorageResource datasetId(UUID datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public CloudPlatform getCloudPlatform() {
        return cloudPlatform;
    }

    public StorageResource cloudPlatform(CloudPlatform cloudPlatform) {
        this.cloudPlatform = cloudPlatform;
        return this;
    }

    public GoogleCloudResource getCloudResource() {
        return cloudResource;
    }

    public StorageResource cloudResource(GoogleCloudResource cloudResource) {
        this.cloudResource = cloudResource;
        return this;
    }

    public GoogleRegion getRegion() {
        return region;
    }

    public StorageResource region(GoogleRegion region) {
        this.region = region;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageResource that = (StorageResource) o;
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
