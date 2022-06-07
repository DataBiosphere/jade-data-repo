package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.dataset.exception.StorageResourceNotFoundException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DatasetSummary {
  private UUID id;
  private String name;
  private String description;
  private UUID defaultProfileId;
  private UUID projectResourceId;
  private UUID applicationDeploymentResourceId;
  private Instant createdDate;
  private List<BillingProfileModel> billingProfiles;
  private List<? extends StorageResource<?, ?>> storage;
  private boolean secureMonitoringEnabled;
  private CloudPlatform cloudPlatform;
  private String dataProject;
  private String storageAccount;
  private String phsId;
  private boolean selfHosted;
  private Object properties;

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

  public List<BillingProfileModel> getBillingProfiles() {
    return billingProfiles;
  }

  public DatasetSummary billingProfiles(List<BillingProfileModel> billingProfiles) {
    this.billingProfiles = billingProfiles;
    return this;
  }

  public BillingProfileModel getDefaultBillingProfile() {
    return billingProfiles.stream()
        .filter(b -> b.getId().equals(defaultProfileId))
        .findFirst()
        .orElseThrow();
  }

  public CloudRegion getStorageResourceRegion(CloudResource storageResource) {
    return getCloudResourceAttribute(storageResource, StorageResource::getRegion);
  }

  public boolean datasetStorageContainsRegion(GoogleRegion region) {
    return storage.stream().anyMatch(sr -> sr.getRegion().equals(region));
  }

  public CloudPlatform getStorageCloudPlatform() {
    // A Dataset should not have both a bucket and a storage account at this point
    return storage.stream()
        .filter(
            sr ->
                sr.getCloudResource() == GoogleCloudResource.BUCKET
                    || sr.getCloudResource() == AzureCloudResource.STORAGE_ACCOUNT)
        .findFirst()
        .map(StorageResource::getCloudPlatform)
        .orElseThrow();
  }

  public CloudPlatform getStorageResourceCloudPlatform(CloudResource cloudResource) {
    return getCloudResourceAttribute(cloudResource, StorageResource::getCloudPlatform);
  }

  private <T> T getCloudResourceAttribute(
      CloudResource cloudResource, Function<StorageResource<?, ?>, T> accessor) {
    return storage.stream()
        .filter(sr -> sr.getCloudResource() == cloudResource)
        .findFirst()
        .map(accessor)
        .orElseThrow(
            () ->
                new StorageResourceNotFoundException(
                    String.format(
                        "%s could not be found for dataset %s", cloudResource.name(), id)));
  }

  public boolean isSecureMonitoringEnabled() {
    return secureMonitoringEnabled;
  }

  public DatasetSummary secureMonitoringEnabled(boolean secureMonitoringEnabled) {
    this.secureMonitoringEnabled = secureMonitoringEnabled;
    return this;
  }

  public CloudPlatform getCloudPlatform() {
    return cloudPlatform;
  }

  public DatasetSummary cloudPlatform(CloudPlatform cloudPlatform) {
    this.cloudPlatform = cloudPlatform;
    return this;
  }

  public String getDataProject() {
    return dataProject;
  }

  public DatasetSummary dataProject(String dataProject) {
    this.dataProject = dataProject;
    return this;
  }

  public String getStorageAccount() {
    return storageAccount;
  }

  public DatasetSummary storageAccount(String storageAccount) {
    this.storageAccount = storageAccount;
    return this;
  }

  public String getPhsId() {
    return phsId;
  }

  public DatasetSummary phsId(String phsId) {
    this.phsId = phsId;
    return this;
  }

  public boolean isSelfHosted() {
    return selfHosted;
  }

  public DatasetSummary selfHosted(boolean selfHosted) {
    this.selfHosted = selfHosted;
    return this;
  }

  public Object getProperties() {
    return properties;
  }

  public DatasetSummary properties(Object properties) {
    this.properties = properties;
    return this;
  }

  public DatasetSummaryModel toModel() {
    return new DatasetSummaryModel()
        .id(getId())
        .name(getName())
        .description(getDescription())
        .createdDate(getCreatedDate().toString())
        .defaultProfileId(getDefaultProfileId())
        .storage(toStorageResourceModel())
        .secureMonitoringEnabled(isSecureMonitoringEnabled())
        .cloudPlatform(getCloudPlatform())
        .dataProject(getDataProject())
        .storageAccount(getStorageAccount())
        .phsId(getPhsId())
        .selfHosted(isSelfHosted());
  }

  List<StorageResourceModel> toStorageResourceModel() {
    return getStorage().stream().map(StorageResource::toModel).collect(Collectors.toList());
  }
}
