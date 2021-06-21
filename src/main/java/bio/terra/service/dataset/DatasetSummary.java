package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.exception.StorageResourceNotFoundException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class DatasetSummary {
    private UUID id;
    private String name;
    private String description;
    private UUID defaultProfileId;
    private UUID projectResourceId;
    private UUID applicationDeploymentResourceId;
    private Instant createdDate;
    private List<? extends StorageResource<?, ?>> storage;

    public UUID getId() {
        return id;
    }

    public DatasetSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DatasetSummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DatasetSummary description(String description) {
        this.description = description;
        return this;
    }

    public UUID getDefaultProfileId() {
        return defaultProfileId;
    }

    public DatasetSummary defaultProfileId(UUID defaultProfileId) {
        this.defaultProfileId = defaultProfileId;
        return this;
    }

    public UUID getProjectResourceId() {
        return projectResourceId;
    }

    public DatasetSummary projectResourceId(UUID projectResourceId) {
        this.projectResourceId = projectResourceId;
        return this;
    }

    public UUID getApplicationDeploymentResourceId() {
        return applicationDeploymentResourceId;
    }

    public DatasetSummary applicationDeploymentResourceId(UUID applicationDeploymentResourceId) {
        this.applicationDeploymentResourceId = applicationDeploymentResourceId;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public DatasetSummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public List<? extends StorageResource<?, ?>> getStorage() {
        return storage;
    }

    public DatasetSummary storage(List<? extends StorageResource<?, ?>> storage) {
        this.storage = storage;
        return this;
    }

    public CloudRegion getStorageResourceRegion(CloudResource storageResource) {
        return getCloudResourceAttribute(storageResource, StorageResource::getRegion);
    }

    public boolean datasetStorageContainsRegion(GoogleRegion region) {
        return storage.stream()
            .anyMatch(sr -> sr.getRegion().equals(region));
    }

    public CloudPlatform getStorageCloudPlatform() {
        return storage.stream().filter(s -> s.getCloudResource() == GoogleCloudResource.BUCKET ||
            s.getCloudResource() == AzureCloudResource.STORAGE_ACCOUNT)
            .findAny()
            .map(s -> getCloudResourceAttribute(s.getCloudResource(), StorageResource::getCloudPlatform))
            .orElseThrow();
    }

    public CloudPlatform getStorageResourceCloudPlatform(CloudResource cloudResource) {
        return getCloudResourceAttribute(cloudResource, StorageResource::getCloudPlatform);
    }

    private <T> T getCloudResourceAttribute(CloudResource cloudResource, Function<StorageResource<?, ?>, T> accessor) {
        return storage.stream()
            .filter(resource -> resource.getCloudResource() == cloudResource)
            .findFirst()
            .map(accessor)
            .orElseThrow(() -> new StorageResourceNotFoundException(
                String.format("%s could not be found for dataset %s", cloudResource.name(), id)));
    }
}
