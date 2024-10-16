package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.LogPrintable;
import bio.terra.common.Relationship;
import bio.terra.model.AssetModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ResourceLocks;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.dataset.exception.InvalidColumnException;
import bio.terra.service.dataset.exception.InvalidTableException;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.google.firestore.FireStoreProject;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
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

  @Override
  public boolean isSnapshot() {
    return false;
  }

  @Override
  public boolean isDataset() {
    return true;
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

  /**
   * @param tableName the string name of the table the column is in
   * @param columnName the string name of the column to fetch
   * @return the column at the specified path
   * @throws InvalidTableException if there is no table of the specified name
   * @throws InvalidColumnException if there is no column in the specified table
   */
  public Column getColumn(String tableName, String columnName) {
    return getTableByName(tableName)
        .orElseThrow(
            () -> new InvalidTableException("No dataset table exists with the name: " + tableName))
        .getColumnByName(columnName)
        .orElseThrow(
            () ->
                new InvalidColumnException(
                    "No column exists in table " + tableName + " with column name: " + columnName));
  }

  public void validateDatasetAssetSpecification(AssetModel assetModel) {
    List<String> errors = new ArrayList<>();
    // Validate Root Table
    String rootTableName = assetModel.getRootTable();
    Optional<DatasetTable> rootTable = getAndValidateTable(rootTableName);
    if (rootTable.isEmpty()) {
      errors.add("Root table " + rootTableName + " does not exist in dataset.");
    } else {
      // Validate Root Column
      if (!rootTable.get().getColumns().stream()
          .anyMatch(c -> c.getName().equals(assetModel.getRootColumn()))) {
        errors.add(
            "Root column "
                + assetModel.getRootColumn()
                + " does not exist in table "
                + rootTableName);
      }
    }

    // Validate Tables
    for (var assetTable : assetModel.getTables()) {
      String currentTableName = assetTable.getName();
      Optional<DatasetTable> currentTable = getAndValidateTable(currentTableName);
      if (currentTable.isEmpty()) {
        errors.add("Table " + currentTableName + " does not exist in dataset.");
      } else {
        List<String> datasetTableColumnNames =
            currentTable.get().getColumns().stream().map(c -> c.getName()).toList();
        assetTable
            .getColumns()
            .forEach(
                assetColumn -> {
                  if (!datasetTableColumnNames.contains(assetColumn)) {
                    errors.add(
                        "Column " + assetColumn + " does not exist in table " + currentTableName);
                  }
                });
      }
    }

    // Follow should reference an existing relationship as defined in the original dataset create
    // query
    for (var assetFollow : ListUtils.emptyIfNull(assetModel.getFollow())) {
      if (!relationships.stream().anyMatch(r -> r.getName().equals(assetFollow))) {
        errors.add(
            "Relationship specified in follow list '"
                + assetFollow
                + "' does not exist in dataset's list of relationships");
      }
    }

    if (errors.size() > 0) {
      throw new InvalidAssetException(
          "Invalid asset create request. See causes list for details.", errors);
    }
  }

  private Optional<DatasetTable> getAndValidateTable(String tableName) {
    return tables.stream()
        .filter(datasetTable -> datasetTable.getName().equals(tableName))
        .findFirst();
  }

  public AssetSpecification getNewAssetSpec(AssetModel assetModel) {
    Map<String, DatasetTable> tablesMap =
        tables.stream()
            .collect(
                Collectors.toMap(
                    datasetTable -> datasetTable.getName(), datasetTable -> datasetTable));
    Map<String, Relationship> relationshipMap =
        relationships.stream()
            .collect(
                Collectors.toMap(
                    relationship -> relationship.getName(), relationship -> relationship));

    AssetSpecification assetSpecification =
        DatasetJsonConversion.assetModelToAssetSpecification(
            assetModel, tablesMap, relationshipMap);
    return assetSpecification;
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

  /**
   * @return whether this dataset has a dedicated GCP service account
   */
  public boolean hasDedicatedGcpServiceAccount() {
    return Optional.ofNullable(projectResource)
        .map(GoogleProjectResource::hasDedicatedServiceAccount)
        .orElse(false);
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
  public CloudPlatform getCloudPlatform() {
    return datasetSummary.getCloudPlatform();
  }

  public boolean hasPredictableFileIds() {
    return datasetSummary.hasPredictableFileIds();
  }

  public Dataset predictableFileIds(boolean predictableFileIds) {
    datasetSummary.predictableFileIds(predictableFileIds);
    return this;
  }

  public List<String> getTags() {
    return datasetSummary.getTags();
  }

  public ResourceLocks getResourceLocks() {
    return datasetSummary.getResourceLocks();
  }

  @Override
  public String toLogString() {
    return String.format("%s (%s)", this.getName(), this.getId());
  }
}
