package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.model.CloudPlatform;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;

@JsonTypeName("azure")
public class AzureStorageResource extends StorageResource<AzureCloudResource, AzureRegion> {

  @JsonCreator
  public AzureStorageResource(
      @JsonProperty("datasetId") UUID datasetId,
      @JsonProperty("cloudResource") AzureCloudResource cloudResource,
      @JsonProperty("region") AzureRegion region) {
    super(datasetId, cloudResource, region);
  }

  @Override
  public CloudPlatform getCloudPlatform() {
    return CloudPlatform.AZURE;
  }
}
