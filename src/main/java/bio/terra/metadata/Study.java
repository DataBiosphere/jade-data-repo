package bio.terra.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Study extends StudySummary {

    private List<StudyTable> tables = Collections.emptyList();
    private List<StudyRelationship> relationships = Collections.emptyList();
    private List<AssetSpecification> assetSpecifications = Collections.emptyList();

    public Study() {
    }

    public Study(StudySummary summary) {
        super(summary);
    }

    public List<StudyTable> getTables() {
        return tables;
    }

    public Study tables(List<StudyTable> tables) {
        this.tables = Collections.unmodifiableList(tables);
        return this;
    }

    public List<StudyRelationship> getRelationships() {
        return relationships;
    }

    public Study relationships(List<StudyRelationship> relationships) {
        this.relationships = Collections.unmodifiableList(relationships);
        return this;
    }

    public List<AssetSpecification> getAssetSpecifications() {
        return assetSpecifications;
    }

    public Study assetSpecifications(List<AssetSpecification> assetSpecifications) {
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

    public Map<UUID, StudyTableColumn> getAllColumnsById() {
        Map<UUID, StudyTableColumn> columns = new HashMap<>();
        getTables().forEach(table -> table.getColumns().forEach(column -> columns.put(column.getId(), column)));
        return columns;
    }

    public Map<UUID, StudyTable> getTablesById() {
        Map<UUID, StudyTable> tables = new HashMap<>();
        getTables().forEach(table -> tables.put(table.getId(), table));
        return tables;
    }

    public Map<UUID, StudyRelationship> getRelationshipsById() {
        Map<UUID, StudyRelationship> relationships = new HashMap<>();
        getRelationships().forEach(relationship -> relationships.put(relationship.getId(), relationship));
        return relationships;
    }

    public Optional<StudyTable> getTableById(UUID id) {
        for (StudyTable tryTable : getTables()) {
            if (tryTable.getId().equals(id)) {
                return Optional.of(tryTable);
            }
        }
        return Optional.empty();
    }
}
