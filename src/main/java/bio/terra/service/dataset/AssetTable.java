package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AssetTable {
  private DatasetTable datasetTable;
  private List<AssetColumn> columns = new ArrayList<>();

  public DatasetTable getTable() {
    return datasetTable;
  }

  public AssetTable datasetTable(DatasetTable datasetTable) {
    this.datasetTable = datasetTable;
    return this;
  }

  public Collection<AssetColumn> getColumns() {
    return columns;
  }

  public AssetTable columns(List<AssetColumn> columns) {
    this.columns = columns;
    return this;
  }

  public List<SynapseColumn> getSynapseColumns() {
    return columns.stream()
        .map(AssetColumn::getDatasetColumn)
        .map(Column::toSynapseColumn)
        .toList();
  }
}
