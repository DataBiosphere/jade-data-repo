package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.LogPrintable;
import bio.terra.common.Relationship;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.google.firestore.FireStoreProject;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public class Dataset implements FSContainerInterface, LogPrintable {

  private final DatasetSummary datasetSummary;
  private List<DatasetTable> tables = Collections.emptyList();
  private List<Relationship> relationships = Collections.emptyList();
  private List<AssetSpecification> assetSpecifications = Collections.emptyList();
  private GoogleProjectResource projectResource;
  private AzureApplicationDeploymentResource applicationDeploymentResource;

  public Dataset() {
    datasetSummary = new DatasetSummary();
  }

  public Dataset(DatasetSummary summary) {
    datasetSummary = summary;
  }

  @Override
  public CollectionType getCollectionType() {
    return CollectionType.DATASET;
  }

  public List<DatasetTable> getTables() {
    return Collections.unmodifiableList(tables);
  }

  public Dataset tables(List<DatasetTable> tables) {
    this.tables = Collections.unmodifiableList(tables);
    return this;
  }

  public List<Relationship> getRelationships() {
    return Collections.unmodifiableList(relationships);
  }

  public Dataset relationships(List<Relationship> relationships) {
    this.relationships = Collections.unmodifiableList(relationships);
    return this;
  }

  public List<AssetSpecification> getAssetSpecifications() {
    return Collections.unmodifiableList(assetSpecifications);
  }

  public Dataset assetSpecifications(List<AssetSpecification> assetSpecifications) {
    this.assetSpecifications = Collections.unmodifiableList(assetSpecifications);
    return this;
  }

  public Optional<AssetSpecification> getAssetSpecificationByName(String name) {
    for (AssetSpecification assetSpecification : getAssetSpecifications()) {
      if (StringUtils.equals(name, assetSpecification.getName())) {
        return Optional.of(assetSpecification);
      }
    }
    return Optional.empty();
  }

  public Optional<AssetSpecification> getAssetSpecificationById(UUID id) {
    for (AssetSpecification assetSpecification : getAssetSpecifications()) {
      if (assetSpecification.getId().equals(id)) {
        return Optional.of(assetSpecification);
      }
    }
    return Optional.empty();
  }

  public Map<UUID, Column> getAllColumnsById() {
    Map<UUID, Column> columns = new HashMap<>();
    getTables()
        .forEach(
            table -> table.getColumns().forEach(column -> columns.put(column.getId(), column)));
    return columns;
  }

  public Map<UUID, DatasetTable> getTablesById() {
    Map<UUID, DatasetTable> tables = new HashMap<>();
    getTables().forEach(table -> tables.put(table.getId(), table));
    return tables;
  }

  public Map<UUID, Relationship> getRelationshipsById() {
    Map<UUID, Relationship> relationships = new HashMap<>();
    getRelationships()
        .forEach(relationship -> relationships.put(relationship.getId(), relationship));
    return relationships;
  }

  public Optional<DatasetTable> getTableById(UUID id) {
    return getTables().stream().filter(table -> table.getId().equals(id)).findFirst();
  }

  public Optional<DatasetTable> getTableByName(String name) {
    return getTables().stream().filter(table -> table.getName().equals(name)).findFirst();
  }

  public DatasetSummary getDatasetSummary() {
    return datasetSummary;
  }

  public UUID getId() {
    return datasetSummary.getId();
  }

  public Dataset id(UUID id) {
    datasetSummary.id(id);
    return this;
  }

  @Override
  public String getName() {
    return datasetSummary.getName();
  }

  public Dataset name(String name) {
    datasetSummary.name(name);
    return this;
  }

  public String getDescription() {
    return datasetSummary.getDescription();
  }

  public Dataset description(String description) {
    datasetSummary.description(description);
    return this;
  }

  public UUID getDefaultProfileId() {
    return datasetSummary.getDefaultProfileId();
  }

  public Dataset defaultProfileId(UUID defaultProfileId) {
    datasetSummary.defaultProfileId(defaultProfileId);
    return this;
  }

  public Instant getCreatedDate() {
    return datasetSummary.getCreatedDate();
  }

  public Dataset createdDate(Instant createdDate) {
    datasetSummary.createdDate(createdDate);
    return this;
  }

  public UUID getProjectResourceId() {
    return datasetSummary.getProjectResourceId();
  }

  public Dataset projectResourceId(UUID projectResourceId) {
    datasetSummary.projectResourceId(projectResourceId);
    return this;
  }

  public UUID getApplicationDeploymentResourceId() {
    return datasetSummary.getApplicationDeploymentResourceId();
  }

  public Dataset applicationDeploymentResourceId(UUID applicationDeploymentResourceId) {
    datasetSummary.applicationDeploymentResourceId(applicationDeploymentResourceId);
    return this;
  }

  public List<? extends StorageResource<?, ?>> getStorage() {
    return datasetSummary.getStorage();
  }

  public Dataset storage(List<? extends StorageResource<?, ?>> storage) {
    datasetSummary.storage(storage);
    return this;
  }

  public GoogleProjectResource getProjectResource() {
    return projectResource;
  }

  public Dataset projectResource(GoogleProjectResource projectResource) {
    this.projectResource = projectResource;
    return this;
  }

  @Override
  public FireStoreProject firestoreConnection() {
    return FireStoreProject.get(getProjectResource().getGoogleProjectId());
  }

  public AzureApplicationDeploymentResource getApplicationDeploymentResource() {
    return applicationDeploymentResource;
  }

  public Dataset applicationDeploymentResource(
      AzureApplicationDeploymentResource applicationDeploymentResource) {
    this.applicationDeploymentResource = applicationDeploymentResource;
    return this;
  }

  public AzureRegion getStorageAccountRegion() {
    return (AzureRegion)
        datasetSummary.getStorageResourceRegion(AzureCloudResource.STORAGE_ACCOUNT);
  }

  public boolean isSecureMonitoringEnabled() {
    return datasetSummary.isSecureMonitoringEnabled();
  }

  public String getPhsId() {
    return datasetSummary.getPhsId();
  }

  public boolean isSelfHosted() {
    return datasetSummary.isSelfHosted();
  }

  public Object getProperties() {
    return datasetSummary.getProperties();
  }

  @Override
  public String toLogString() {
    return String.format("%s (%s)", this.getName(), this.getId());
  }
}
