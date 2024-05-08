package bio.terra.service.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AssetSpecification {
  private UUID id;
  private String name;
  private AssetTable rootTable;
  private AssetColumn rootColumn;
  private List<AssetTable> assetTables = new ArrayList<>();
  private List<AssetRelationship> assetRelationships = new ArrayList<>();

  public AssetColumn getRootColumn() {
    return rootColumn;
  }

  public AssetSpecification rootColumn(AssetColumn rootColumn) {
    this.rootColumn = rootColumn;
    return this;
  }

  public UUID getId() {
    return id;
  }

  public AssetSpecification id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public AssetSpecification name(String name) {
    this.name = name;
    return this;
  }

  public AssetTable getRootTable() {
    return rootTable;
  }

  public AssetSpecification rootTable(AssetTable rootTable) {
    this.rootTable = rootTable;
    return this;
  }

  public List<AssetRelationship> getAssetRelationships() {
    return Collections.unmodifiableList(assetRelationships);
  }

  public AssetSpecification assetRelationships(List<AssetRelationship> assetRelationships) {
    this.assetRelationships = assetRelationships;
    return this;
  }

  public List<AssetTable> getAssetTables() {
    return Collections.unmodifiableList(assetTables);
  }

  public AssetSpecification assetTables(List<AssetTable> includedTables) {
    this.assetTables = includedTables;
    return this;
  }

  public AssetTable getAssetTableByName(String tableName) {
    return this.assetTables.stream()
        .filter(at -> at.getTable().getName().equals(tableName))
        .findFirst()
        .orElseThrow();
  }
}
