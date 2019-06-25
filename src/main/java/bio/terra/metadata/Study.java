package bio.terra.metadata;

import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Study {

    private final StudySummary studySummary;
    private List<Table> tables = Collections.emptyList();
    private List<StudyRelationship> relationships = Collections.emptyList();
    private List<AssetSpecification> assetSpecifications = Collections.emptyList();

    public Study() {
        studySummary = new StudySummary();
    }

    public Study(StudySummary summary) {
        studySummary = summary;
    }

    public List<Table> getTables() {
        return Collections.unmodifiableList(tables);
    }

    public Study tables(List<Table> tables) {
        this.tables = Collections.unmodifiableList(tables);
        return this;
    }

    public List<StudyRelationship> getRelationships() {
        return Collections.unmodifiableList(relationships);
    }

    public Study relationships(List<StudyRelationship> relationships) {
        this.relationships = Collections.unmodifiableList(relationships);
        return this;
    }

    public List<AssetSpecification> getAssetSpecifications() {
        return Collections.unmodifiableList(assetSpecifications);
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

    public Map<UUID, StudyRelationship> getRelationshipsById() {
        Map<UUID, StudyRelationship> relationships = new HashMap<>();
        getRelationships().forEach(relationship -> relationships.put(relationship.getId(), relationship));
        return relationships;
    }

    public Optional<Table> getTableById(UUID id) {
        return getTables().stream().filter(table -> table.getId().equals(id)).findFirst();
    }

    public Optional<Table> getTableByName(String name) {
        return getTables().stream().filter(table -> table.getName().equals(name)).findFirst();
    }

    public StudySummary getStudySummary() {
        return studySummary;
    }

    public UUID getId() {
        return studySummary.getId();
    }

    public Study id(UUID id) {
        studySummary.id(id);
        return this;
    }

    public String getName() {
        return studySummary.getName();
    }

    public Study name(String name) {
        studySummary.name(name);
        return this;
    }

    public String getDescription() {
        return studySummary.getDescription();
    }

    public Study description(String description) {
        studySummary.description(description);
        return this;
    }

    public UUID getDefaultProfileId() {
        return studySummary.getDefaultProfileId();
    }

    public Study defaultProfileId(UUID defaultProfileId) {
        studySummary.defaultProfileId(defaultProfileId);
        return this;
    }

    public List<UUID> getAdditionalProfileIds() {
        return studySummary.getAdditionalProfileIds();
    }

    public Study additionalProfileIds(List<UUID> additionalProfileIds) {
        studySummary.additionalProfileIds(additionalProfileIds);
        return this;
    }

    public Instant getCreatedDate() {
        return studySummary.getCreatedDate();
    }

    public Study createdDate(Instant createdDate) {
        studySummary.createdDate(createdDate);
        return this;
    }
}
