package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.UUID;

@JsonTypeName("gcp")
public class GoogleStorageResource extends StorageResource<GoogleCloudResource, GoogleRegion> {

    public GoogleStorageResource(UUID datasetId, GoogleCloudResource cloudResource, GoogleRegion region) {
        super(datasetId, cloudResource, region);
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.GCP;
    }
}
