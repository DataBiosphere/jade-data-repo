package bio.terra.service.snapshot;

import bio.terra.service.dataset.DatasetTable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SnapshotMapTable {
  private UUID id;
  private DatasetTable fromTable;
  private SnapshotTable toTable;
  private List<SnapshotMapColumn> snapshotMapColumns = Collections.emptyList();

  public UUID getId() {
    return id;
  }

  public SnapshotMapTable id(UUID id) {
    this.id = id;
    return this;
  }

  public DatasetTable getFromTable() {
    return fromTable;
  }

  public SnapshotMapTable fromTable(DatasetTable fromTable) {
    this.fromTable = fromTable;
    return this;
  }

  public SnapshotTable getToTable() {
    return toTable;
  }

  public SnapshotMapTable toTable(SnapshotTable toTable) {
    this.toTable = toTable;
    return this;
  }

  public List<SnapshotMapColumn> getSnapshotMapColumns() {
    return snapshotMapColumns;
  }

  public SnapshotMapTable snapshotMapColumns(List<SnapshotMapColumn> snapshotMapColumns) {
    this.snapshotMapColumns = snapshotMapColumns;
    return this;
  }
}
