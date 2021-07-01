package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;

@JsonTypeName("azure")
public class AzureStorageResource extends StorageResource<AzureCloudResource, AzureRegion> {

  public AzureStorageResource(
      UUID datasetId, AzureCloudResource cloudResource, AzureRegion region) {
    super(datasetId, cloudResource, region);
  }

  @Override
  public CloudPlatform getCloudPlatform() {
    return CloudPlatform.AZURE;
  }
}
