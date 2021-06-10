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
public interface StorageResource {

    UUID getDatasetId();

    StorageResource datasetId(UUID datasetId);

    CloudPlatform getCloudPlatform();

    CloudResource getCloudResource();

    StorageResource cloudResource(CloudResource cloudResource);

    CloudRegion getRegion();

    StorageResource region(CloudRegion region);

    default StorageResourceModel toModel() {
        return new StorageResourceModel()
            .cloudPlatform(getCloudPlatform())
            .cloudResource(getCloudResource().toString())
            .region(getRegion().toString());
    }

    static GoogleStorageResource getGoogleInstance() {
        return new GoogleStorageResource();
    }

    static AzureStorageResource getAzureInstance() {
        return new AzureStorageResource();
    }
}
