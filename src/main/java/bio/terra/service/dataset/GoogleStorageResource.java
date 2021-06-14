package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;
import java.util.UUID;

@JsonTypeName("gcp")
public class GoogleStorageResource extends StorageResource<GoogleRegion, GoogleCloudResource> {

    public GoogleStorageResource(GoogleRegion region, GoogleCloudResource resource) {
        super(null, resource, region);
    }

    public GoogleStorageResource(UUID datasetId, GoogleCloudResource cloudResource, GoogleRegion region) {
        super(datasetId, cloudResource, region);
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.GCP;
    }
}
