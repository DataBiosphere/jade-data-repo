package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.common.Table;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Dataset implements FSContainerInterface {

    private final DatasetSummary datasetSummary;
    private List<DatasetTable> tables = Collections.emptyList();
    private List<DatasetRelationship> relationships = Collections.emptyList();
    private List<AssetSpecification> assetSpecifications = Collections.emptyList();

    public Dataset() {
        datasetSummary = new DatasetSummary();
    }

    public Dataset(DatasetSummary summary) {
        datasetSummary = summary;
    }

    public List<DatasetTable> getTables() {
        return Collections.unmodifiableList(tables);
    }

    public Dataset tables(List<DatasetTable> tables) {
        this.tables = Collections.unmodifiableList(tables);
        return this;
    }

    public List<DatasetRelationship> getRelationships() {
        return Collections.unmodifiableList(relationships);
    }

    public Dataset relationships(List<DatasetRelationship> relationships) {
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
        getTables().forEach(table -> table.getColumns().forEach(column -> columns.put(column.getId(), column)));
        return columns;
    }

    public Map<UUID, Table> getTablesById() {
        Map<UUID, Table> tables = new HashMap<>();
        getTables().forEach(table -> tables.put(table.getId(), table));
        return tables;
    }

    public Map<UUID, DatasetRelationship> getRelationshipsById() {
        Map<UUID, DatasetRelationship> relationships = new HashMap<>();
        getRelationships().forEach(relationship -> relationships.put(relationship.getId(), relationship));
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

    public List<UUID> getAdditionalProfileIds() {
        return datasetSummary.getAdditionalProfileIds();
    }

    public Dataset additionalProfileIds(List<UUID> additionalProfileIds) {
        datasetSummary.additionalProfileIds(additionalProfileIds);
        return this;
    }

    public Instant getCreatedDate() {
        return datasetSummary.getCreatedDate();
    }

    public Dataset createdDate(Instant createdDate) {
        datasetSummary.createdDate(createdDate);
        return this;
    }

}
