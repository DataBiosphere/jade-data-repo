package bio.terra.service.snapshot;

import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.StorageResource;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class SnapshotSummary {
  private UUID id;
  private String name;
  private String description;
  private Instant createdDate;
  private UUID profileId;
  private List<StorageResource> storage;
  private boolean secureMonitoringEnabled;
  private CloudPlatform cloudPlatform;
  private List<String> dataProjects;
  private List<String> storageAccounts;

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

  public List<String> getDataProjects() {
    return dataProjects;
  }

  public SnapshotSummary dataProjects(List<String> dataProjects) {
    this.dataProjects = dataProjects;
    return this;
  }

  public List<String> getStorageAccounts() {
    return storageAccounts;
  }

  public SnapshotSummary storageAccounts(List<String> storageAccounts) {
    this.storageAccounts = storageAccounts;
    return this;
  }
}
