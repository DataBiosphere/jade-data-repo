package bio.terra.service.snapshot;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.google.firestore.FireStoreProject;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Snapshot implements FSContainerInterface {
  private UUID id;
  private String name;
  private String description;
  private Instant createdDate;
  private List<SnapshotTable> tables = Collections.emptyList();
  private List<SnapshotSource> snapshotSources = Collections.emptyList();
  private UUID profileId;
  private UUID projectResourceId;
  private GoogleProjectResource projectResource;
  private AzureStorageAccountResource storageAccountResource;
  private List<Relationship> relationships = Collections.emptyList();

  @Override
  public CollectionType getCollectionType() {
    return CollectionType.SNAPSHOT;
  }

  public UUID getId() {
    return id;
  }

  public Snapshot id(UUID id) {
    this.id = id;
    return this;
  }

  @Override
  public String getName() {
    return name;
  }

  public Snapshot name(String name) {
    this.name = name;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public Snapshot description(String description) {
    this.description = description;
    return this;
  }

  public Instant getCreatedDate() {
    return createdDate;
  }

  public Snapshot createdDate(Instant createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  public List<SnapshotTable> getTables() {
    return tables;
  }

  public Snapshot snapshotTables(List<SnapshotTable> tables) {
    this.tables = tables;
    return this;
  }

  public List<SnapshotSource> getSnapshotSources() {
    return snapshotSources;
  }

  public Snapshot snapshotSources(List<SnapshotSource> snapshotSources) {
    this.snapshotSources = snapshotSources;
    return this;
  }

  // TODO: When we support more than one dataset per snapshot, all uses of this will need
  // refactoring.
  public SnapshotSource getFirstSnapshotSource() {
    if (snapshotSources.isEmpty()) {
      throw new CorruptMetadataException("Snapshot sources should never be empty!");
    }
    return snapshotSources.get(0);
  }

  public Optional<SnapshotTable> getTableById(UUID id) {
    for (SnapshotTable tryTable : getTables()) {
      if (tryTable.getId().equals(id)) {
        return Optional.of(tryTable);
      }
    }
    return Optional.empty();
  }

  public UUID getProfileId() {
    return profileId;
  }

  public Snapshot profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public UUID getProjectResourceId() {
    return projectResourceId;
  }

  public Snapshot projectResourceId(UUID projectResourceId) {
    this.projectResourceId = projectResourceId;
    return this;
  }

  public GoogleProjectResource getProjectResource() {
    return projectResource;
  }

  public Snapshot projectResource(GoogleProjectResource projectResource) {
    this.projectResource = projectResource;
    return this;
  }

  public AzureStorageAccountResource getStorageAccountResource() {
    return storageAccountResource;
  }

  public Snapshot storageAccountResource(AzureStorageAccountResource storageAccountResource) {
    this.storageAccountResource = storageAccountResource;
    return this;
  }

  public List<Relationship> getRelationships() {
    return relationships;
  }

  public Snapshot relationships(List<Relationship> relationships) {
    this.relationships = relationships;
    return this;
  }

  public Map<UUID, SnapshotTable> getTablesById() {
    return getTables().stream()
        .collect(Collectors.toMap(SnapshotTable::getId, Function.identity()));
  }

  public Map<UUID, Column> getAllColumnsById() {
    Map<UUID, Column> result = new HashMap<>();
    getTables().forEach(t -> t.getColumns().forEach(c -> result.put(c.getId(), c)));
    return result;
  }

  @Override
  public FireStoreProject firestoreConnection() {
    String datasetProjectId =
        getFirstSnapshotSource().getDataset().getProjectResource().getGoogleProjectId();
    return FireStoreProject.get(datasetProjectId);
  }

  public AzureRegion getStorageAccountRegion() {
    return getFirstSnapshotSource().getDataset().getStorageAccountRegion();
  }

  public boolean isSecureMonitoringEnabled() {
    return getFirstSnapshotSource().getDataset().isSecureMonitoringEnabled();
  }
}
