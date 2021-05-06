package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;

public class StorageResource {

    private CloudPlatform cloudPlatform;
    private GoogleCloudResource cloudResource;
    private GoogleRegion region;

    public StorageResourceModel toModel() {
        return new StorageResourceModel()
            .cloudPlatform(this.cloudPlatform)
            .cloudResource(this.cloudResource.toString())
            .region(this.region.toString());
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
}
