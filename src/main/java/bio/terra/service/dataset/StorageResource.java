package bio.terra.service.dataset;

import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;

public class StorageResource {

    private CloudPlatform cloudPlatform;
    private String cloudResource;
    private String region;

    public StorageResourceModel toModel() {
        return new StorageResourceModel()
            .cloudPlatform(this.cloudPlatform)
            .cloudResource(this.cloudResource)
            .region(this.region);
    }

    public CloudPlatform getCloudPlatform() {
        return cloudPlatform;
    }

    public StorageResource cloudPlatform(CloudPlatform cloudPlatform) {
        this.cloudPlatform = cloudPlatform;
        return this;
    }

    public String getCloudResource() {
        return cloudResource;
    }

    public StorageResource cloudResource(String cloudResource) {
        this.cloudResource = cloudResource;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public StorageResource region(String region) {
        this.region = region;
        return this;
    }
}
