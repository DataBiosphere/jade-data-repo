package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.common.Column;
import bio.terra.datarepo.common.Table;
import java.util.UUID;

public class AssetColumn {
  private UUID id;
  private Table datasetTable;
  private Column datasetColumn;

  public UUID getId() {
    return id;
  }

  public AssetColumn id(UUID id) {
    this.id = id;
    return this;
  }

  public Column getDatasetColumn() {
    return datasetColumn;
  }

  public AssetColumn datasetColumn(Column datasetColumn) {
    this.datasetColumn = datasetColumn;
    return this;
  }

  public Table getTable() {
    return datasetTable;
  }

  public AssetColumn datasetTable(Table datasetTable) {
    this.datasetTable = datasetTable;
    return this;
  }
}
