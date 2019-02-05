package bio.terra.metadata;

import bio.terra.model.*;

import java.time.Instant;
import java.util.*;

public class Study {

    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;

    private Map<String, StudyTable> tables = new HashMap<>();
    private Map<String, StudyRelationship> relationships = new HashMap<>();
    private Map<String, AssetSpecification> assetSpecifications = new HashMap<>();

    public Study() {}

    public Study(String name,
                 String description,
                 Map<String, StudyTable> tables,
                 Map<String, StudyRelationship> relationships,
                 Map<String, AssetSpecification> assetSpecifications) {
        this.name = name;
        this.description = description;
        this.tables = tables;
        this.relationships = relationships;
        this.assetSpecifications = assetSpecifications;
    }

    public UUID getId() { return id; }
    public Study setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public Study setName(String name) { this.name = name; return this; }

    public String getDescription() {
        return description;
    }
    public Study setDescription(String description) { this.description = description; return this; }

    public Instant getCreatedDate() {
        return createdDate;
    }
    public Study setCreatedDate(Instant createdDate) { this.createdDate = createdDate; return this; }

    public Collection<StudyTable> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }
    public Study setTables(Map<String, StudyTable> tables) { this.tables = tables; return this; }

    public Map<String, StudyRelationship> getRelationships() {
        return Collections.unmodifiableMap(relationships);
    }
    public Study setRelationships(Map<String, StudyRelationship> relationships) {
        this.relationships = relationships;
        return this;
    }

    public Map<String, AssetSpecification> getAssetSpecifications() {
        return Collections.unmodifiableMap(assetSpecifications);
    }
    public Study setAssetSpecifications(Map<String, AssetSpecification> assetSpecifications) {
        this.assetSpecifications = assetSpecifications;
        return this;
    }
}
