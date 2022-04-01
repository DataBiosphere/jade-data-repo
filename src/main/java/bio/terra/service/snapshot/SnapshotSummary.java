package bio.terra.service.snapshot;

import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.dataset.StorageResource;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotSummary {
  private UUID id;
  private String name;
  private String description;
  private Instant createdDate;
  private UUID profileId;
  private List<StorageResource> storage;
  private boolean secureMonitoringEnabled;
  private CloudPlatform cloudPlatform;
  private String dataProject;
  private String storageAccount;
  private String consentCode;
  private String phsId;
  private boolean selfHosted;

  public UUID getId() {
    return id;
  }

  public SnapshotSummary id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public SnapshotSummary name(String name) {
    this.name = name;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public SnapshotSummary description(String description) {
    this.description = description;
    return this;
  }

  public Instant getCreatedDate() {
    return createdDate;
  }

  public SnapshotSummary createdDate(Instant createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public SnapshotSummary profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public List<StorageResource> getStorage() {
    return storage;
  }

  public SnapshotSummary storage(List<StorageResource> storage) {
    this.storage = storage;
    return this;
  }

  public boolean isSecureMonitoringEnabled() {
    return secureMonitoringEnabled;
  }

  public SnapshotSummary secureMonitoringEnabled(boolean secureMonitoringEnabled) {
    this.secureMonitoringEnabled = secureMonitoringEnabled;
    return this;
  }

  public CloudPlatform getCloudPlatform() {
    return cloudPlatform;
  }

  public SnapshotSummary cloudPlatform(CloudPlatform cloudPlatform) {
    this.cloudPlatform = cloudPlatform;
    return this;
  }

  public String getDataProject() {
    return dataProject;
  }

  public SnapshotSummary dataProject(String dataProject) {
    this.dataProject = dataProject;
    return this;
  }

  public String getStorageAccount() {
    return storageAccount;
  }

  public SnapshotSummary storageAccount(String storageAccount) {
    this.storageAccount = storageAccount;
    return this;
  }

  public String getConsentCode() {
    return consentCode;
  }

  public SnapshotSummary consentCode(String consentCode) {
    this.consentCode = consentCode;
    return this;
  }

  public String getPhsId() {
    return phsId;
  }

  public SnapshotSummary phsId(String phsId) {
    this.phsId = phsId;
    return this;
  }

  public boolean isSelfHosted() {
    return selfHosted;
  }

  public SnapshotSummary selfHosted(boolean selfHosted) {
    this.selfHosted = selfHosted;
    return this;
  }

  public SnapshotSummaryModel toModel() {
    return new SnapshotSummaryModel()
        .id(getId())
        .name(getName())
        .description(getDescription())
        .createdDate(getCreatedDate().toString())
        .profileId(getProfileId())
        .storage(toStorageResourceModel())
        .secureMonitoringEnabled(isSecureMonitoringEnabled())
        .cloudPlatform(getCloudPlatform())
        .dataProject(getDataProject())
        .storageAccount(getStorageAccount())
        .consentCode(getConsentCode())
        .phsId(getPhsId())
        .selfHosted(isSelfHosted());
  }

  private List<StorageResourceModel> toStorageResourceModel() {
    return getStorage().stream().map(StorageResource::toModel).collect(Collectors.toList());
  }
}
