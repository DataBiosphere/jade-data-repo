package bio.terra.service.dataset;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.model.CloudPlatform;
import bio.terra.model.StorageResourceModel;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "cloudPlatform")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AzureStorageResource.class, name = "azure"),
    @JsonSubTypes.Type(value = GoogleStorageResource.class, name = "gcp")
})
public interface StorageResource<Region extends CloudRegion, Resource extends CloudResource> {

    UUID getDatasetId();

    StorageResource<Region, Resource> datasetId(UUID datasetId);

    CloudPlatform getCloudPlatform();

    Resource getCloudResource();

    StorageResource<Region, Resource> cloudResource(Resource cloudResource);

    Region getRegion();

    StorageResource<Region, Resource> region(Region region);

    default StorageResourceModel toModel() {
        return new StorageResourceModel()
            .cloudPlatform(getCloudPlatform())
            .cloudResource(getCloudResource().getValue())
            .region(getRegion().getValue());
    }

}
