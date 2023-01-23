package bio.terra.service.snapshot;

import bio.terra.common.Table;
import bio.terra.common.exception.NotFoundException;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class SnapshotSource {
  private UUID id;
  private Snapshot snapshot;
  private Dataset dataset;
  private AssetSpecification assetSpecification;
  private List<SnapshotMapTable> snapshotMapTables = Collections.emptyList();

  public Snapshot getSnapshot() {
    return snapshot;
  }

  public SnapshotSource snapshot(Snapshot snapshot) {
    this.snapshot = snapshot;
    return this;
  }

  public Dataset getDataset() {
    return dataset;
  }

  public SnapshotSource dataset(Dataset dataset) {
    this.dataset = dataset;
    return this;
  }

  public AssetSpecification getAssetSpecification() {
    return assetSpecification;
  }

  public SnapshotSource assetSpecification(AssetSpecification assetSpecification) {
    this.assetSpecification = assetSpecification;
    return this;
  }

  public List<SnapshotMapTable> getSnapshotMapTables() {
    return snapshotMapTables;
  }

  public SnapshotSource snapshotMapTables(List<SnapshotMapTable> snapshotMapTables) {
    this.snapshotMapTables = snapshotMapTables;
    return this;
  }

  public UUID getId() {

    return id;
  }

  public SnapshotSource id(UUID id) {
    this.id = id;
    return this;
  }

  /**
   * For this source, look up the source table that maps to the provided destination table name.
   *
   * @param tableName the name of the destination table
   * @return an Optional that will be empty if no match is found for the table name
   */
  public Optional<Table> reverseTableLookup(String tableName) {
    return getSnapshotMapTables().stream()
        .filter(mapTable -> mapTable.getToTable().getName().equals(tableName))
        .findFirst()
        .map(SnapshotMapTable::getFromTable);
  }

  public DatasetTable getDatasetTable(String tableName) {
    return this.getDataset()
        .getTableByName(tableName)
        .orElseThrow(
            () ->
                new NotFoundException(
                    String.format(
                        "Table %s was not found in dataset %s",
                        tableName, this.getDataset().toLogString())));
  }
}
