package bio.terra.datarepo.service.snapshot;

import bio.terra.datarepo.common.Column;
import bio.terra.datarepo.common.Table;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SnapshotTable implements Table {
  private UUID id;
  private String name;
  private List<Column> columns = Collections.emptyList();
  private Long rowCount;

  public UUID getId() {
    return id;
  }

  public SnapshotTable id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public SnapshotTable name(String name) {
    this.name = name;
    return this;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public SnapshotTable columns(List<Column> columns) {
    this.columns = columns;
    return this;
  }

  @Override
  public Long getRowCount() {
    return rowCount;
  }

  public SnapshotTable rowCount(long rowCount) {
    this.rowCount = rowCount;
    return this;
  }
}
