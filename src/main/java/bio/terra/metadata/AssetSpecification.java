package bio.terra.metadata;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AssetSpecification {
    private UUID id;
    private String name;
    private AssetTable rootTable;

    public AssetColumn getRootColumn() {
        return rootColumn;
    }

    public AssetSpecification rootColumn(AssetColumn rootColumn) {
        this.rootColumn = rootColumn;
        return this;
    }

    private AssetColumn rootColumn;
    private List<AssetTable> assetTables = new ArrayList<>();
    private List<AssetRelationship> assetRelationships = new ArrayList<>();

    public UUID getId() {
        return id;
    }
    public AssetSpecification setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public AssetSpecification setName(String name) { this.name = name; return this; }

    public AssetTable getRootTable() { return rootTable; }
    public AssetSpecification setRootTable(AssetTable rootTable) { this.rootTable = rootTable; return this; }

    public List<AssetRelationship> getAssetRelationships() { return Collections.unmodifiableList(assetRelationships); }
    public AssetSpecification setAssetRelationships(List<AssetRelationship> assetRelationships) {
        this.assetRelationships = assetRelationships;
        return this;
    }

    public List<AssetTable> getAssetTables() {
        return Collections.unmodifiableList(assetTables);
    }
    public AssetSpecification setAssetTables(List<AssetTable> includedTables) {
        this.assetTables = includedTables;
        return this;
    }
}
