package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.app.model.AzureCloudResource;
import bio.terra.datarepo.app.model.AzureRegion;
import bio.terra.datarepo.model.CloudPlatform;
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
