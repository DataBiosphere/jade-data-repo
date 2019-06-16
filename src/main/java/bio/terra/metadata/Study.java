package bio.terra.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Study extends StudySummary {

    private List<Table> tables = Collections.emptyList();
    private List<StudyRelationship> relationships = Collections.emptyList();
    private List<AssetSpecification> assetSpecifications = Collections.emptyList();
    private BillingProfile defaultProfile;
    private List<BillingProfile> additionalProfiles = Collections.emptyList();

    public Study() {
    }

    public Study(StudySummary summary) {
        super(summary);
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

    public BillingProfile getDefaultProfile() {
        return this.defaultProfile;
    }

    public Study defaultProfile(BillingProfile defaultProfile) {
        this.defaultProfile = defaultProfile;
        return this;
    }

    public List<BillingProfile> getAdditionalProfiles() {
        return Collections.unmodifiableList(additionalProfiles);
    }

    public Study additionalProfiles(List<BillingProfile> additionalProfileIds) {
        this.additionalProfiles = Collections.unmodifiableList(additionalProfileIds);
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

}
