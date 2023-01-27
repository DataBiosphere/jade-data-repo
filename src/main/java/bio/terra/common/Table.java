package bio.terra.common;

import java.util.*;

/** Generic class for tables. It can be used for both dataset and snapshot tables. */
public interface Table {
  UUID getId();

  String getName();

  List<Column> getColumns();

  Long getRowCount();

  List<Column> getPrimaryKey();

  default Optional<Column> getColumnById(UUID id) {
    for (Column tryColumn : getColumns()) {
      if (tryColumn.getId().equals(id)) {
        return Optional.of(tryColumn);
      }
    }
    return Optional.empty();
  }

  // Build a name to column map
  default Map<String, Column> getColumnsMap() {
    Map<String, Column> columnMap = new HashMap<>();
    getColumns().forEach(column -> columnMap.put(column.getName(), column));
    return Collections.unmodifiableMap(columnMap);
  }

  /**
   * @return Columns formatted for use with Azure Synapse
   */
  default List<SynapseColumn> getSynapseColumns() {
    return getColumns().stream().map(Column::toSynapseColumn).toList();
  }
}
