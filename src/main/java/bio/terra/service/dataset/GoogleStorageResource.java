package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;
import java.util.UUID;

@JsonTypeName("gcp")
public class GoogleStorageResource implements StorageResource<GoogleRegion, GoogleCloudResource> {
    private UUID datasetId;
    private GoogleCloudResource cloudResource;
    private GoogleRegion region;

    @Override
    public UUID getDatasetId() {
        return datasetId;
    }

    @Override
    public GoogleStorageResource datasetId(UUID datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.GCP;
    }

    @Override
    public GoogleCloudResource getCloudResource() {
        return cloudResource;
    }

    @Override
    public GoogleStorageResource cloudResource(GoogleCloudResource cloudResource) {
        this.cloudResource = cloudResource;
        return this;
    }

    @Override
    public GoogleRegion getRegion() {
        return region;
    }

    @Override
    public GoogleStorageResource region(GoogleRegion region) {
        this.region = region;
        return this;
    }

    @Override
    public StorageResourceModel toModel() {
        GoogleRegion regionOverride = region;
        if (cloudResource == GoogleCloudResource.FIRESTORE) {
            regionOverride = region.getRegionOrFallbackFirestoreRegion();
        } else if (cloudResource == GoogleCloudResource.BUCKET) {
            regionOverride = region.getRegionOrFallbackBucketRegion();
        }
        return new StorageResourceModel()
            .cloudPlatform(getCloudPlatform())
            .cloudResource(getCloudResource().toString())
            .region(regionOverride.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GoogleStorageResource that = (GoogleStorageResource) o;
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
        return "GoogleStorageResource{" +
            "datasetId=" + getDatasetId() +
            ", cloudPlatform=" + getCloudPlatform() +
            ", cloudResource=" + getCloudResource() +
            ", region=" + getRegion() +
            '}';
    }
}
