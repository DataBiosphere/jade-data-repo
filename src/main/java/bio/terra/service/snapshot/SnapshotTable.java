package bio.terra.service.snapshot;

import bio.terra.common.Column;
import bio.terra.common.Table;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class SnapshotTable implements Table {
  private UUID id;
  private String name;
  private List<Column> columns = Collections.emptyList();
  private List<Column> primaryKey = Collections.emptyList();
  private Long rowCount;

  public UUID getId() {
    return id;
  }

  public SnapshotTable id(UUID id) {
    this.id = id;
    return this;
  }

  @Override
  public String getName() {
    return name;
  }

  public SnapshotTable name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public List<Column> getColumns() {
    return columns;
  }

  public Optional<Column> getColumnByName(String columnName) {
    return this.columns.stream().filter(t -> t.getName().equals(columnName)).findFirst();
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

  @Override
  public List<Column> getPrimaryKey() {
    return primaryKey;
  }

  public SnapshotTable primaryKey(List<Column> primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotTable that = (SnapshotTable) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(columns, that.columns)
        && Objects.equals(primaryKey, that.primaryKey)
        && Objects.equals(rowCount, that.rowCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, columns, primaryKey, rowCount);
  }
}
