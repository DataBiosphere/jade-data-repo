package bio.terra.metadata;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AssetSpecification {
    private UUID id;
    private String name;
    private StudyTable rootTable;
    private List<StudyTable> includedTables = new ArrayList<>();
    private List<AssetColumn> assetColumns = new ArrayList<>();
    private List<AssetRelationship> assetRelationships = new ArrayList<>();

    public AssetSpecification() {}

    public UUID getId() {
        return id;
    }
    public AssetSpecification setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public AssetSpecification setName(String name) { this.name = name; return this; }

    public StudyTable getRootTable() {
        return rootTable;
    }
    public AssetSpecification setRootTable(StudyTable rootTable) { this.rootTable = rootTable; return this; }

    public List<AssetColumn> getAssetColumns() {
        return Collections.unmodifiableList(assetColumns);
    }
    public AssetSpecification setAssetColumns(List<AssetColumn> assetColumns) {
        this.assetColumns = assetColumns;
        return this;
    }

    public List<AssetRelationship> getAssetRelationships() {
        return Collections.unmodifiableList(assetRelationships);
    }
    public AssetSpecification setAssetRelationships(List<AssetRelationship> assetRelationships) {
        this.assetRelationships = assetRelationships;
        return this;
    }

    public List<StudyTable> getIncludedTables() {
        return Collections.unmodifiableList(includedTables);
    }
    public AssetSpecification setIncludedTables(List<StudyTable> includedTables) {
        this.includedTables = includedTables;
        return this;
    }
}
