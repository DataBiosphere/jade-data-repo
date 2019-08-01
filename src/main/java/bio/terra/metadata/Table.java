package bio.terra.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Generic class for tables. It can be used for both study and dataset tables.
 */
public class Table {
    private UUID id;
    private String name;
    private List<Column> primaryKey;
    private List<Column> columns = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public Table id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Table name(String name) {
        this.name = name;
        return this;
    }

    public List<Column> getPrimaryKey() {
        return primaryKey;
    }

    public Table primaryKey(List<Column> primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Table columns(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public Optional<Column> getColumnById(UUID id) {
        for (Column tryColumn : getColumns()) {
            if (tryColumn.getId().equals(id)) {
                return Optional.of(tryColumn);
            }
        }
        return Optional.empty();
    }

    // Build a name to column map
    public Map<String, Column> getColumnsMap() {
        Map<String, Column> columnMap = new HashMap<>();
        columns.forEach(column -> columnMap.put(column.getName(), column));
        return Collections.unmodifiableMap(columnMap);
    }
}
