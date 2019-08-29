package bio.terra.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Generic class for tables. It can be used for both dataset and snapshot tables.
 */
public interface Table {
    UUID getId();
    String getName();
    List<Column> getColumns();

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
}
