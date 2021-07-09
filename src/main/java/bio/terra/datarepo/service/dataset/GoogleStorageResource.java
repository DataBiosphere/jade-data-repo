package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.app.model.GoogleCloudResource;
import bio.terra.datarepo.app.model.GoogleRegion;
import bio.terra.datarepo.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;

@JsonTypeName("gcp")
public class GoogleStorageResource extends StorageResource<GoogleCloudResource, GoogleRegion> {

  public GoogleStorageResource(
      UUID datasetId, GoogleCloudResource cloudResource, GoogleRegion region) {
    super(datasetId, cloudResource, region);
  }

  @Override
  public CloudPlatform getCloudPlatform() {
    return CloudPlatform.GCP;
  }
}
