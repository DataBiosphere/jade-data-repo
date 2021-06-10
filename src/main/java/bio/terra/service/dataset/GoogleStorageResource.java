package bio.terra.service.dataset;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;
import java.util.UUID;

@JsonTypeName("gcp")
public class GoogleStorageResource implements StorageResource {
    private UUID datasetId;
    private final CloudPlatform cloudPlatform;
    private GoogleCloudResource cloudResource;
    private GoogleRegion region;

    @JsonCreator
    public GoogleStorageResource() {
        cloudPlatform = CloudPlatform.GCP;
    }

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
        return cloudPlatform;
    }

    @Override
    public CloudResource getCloudResource() {
        return cloudResource;
    }

    @Override
    public GoogleStorageResource cloudResource(CloudResource cloudResource) {
        this.cloudResource = (GoogleCloudResource) cloudResource;
        return this;
    }

    @Override
    public CloudRegion getRegion() {
        return region;
    }

    @Override
    public GoogleStorageResource region(CloudRegion region) {
        this.region = (GoogleRegion) region;
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
