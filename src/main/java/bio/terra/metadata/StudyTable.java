package bio.terra.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StudyTable {

    private String name;
    private List<StudyTableColumn> columns = new ArrayList<>();
    private UUID id;

    public UUID getId() { return id; }
    public StudyTable setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public StudyTable setName(String name) { this.name = name; return this; }

    public Collection<StudyTableColumn> getColumns() { return columns; }
    public StudyTable setColumns(List<StudyTableColumn> columns) { this.columns = columns; return this; }

    public Map<String, StudyTableColumn> getColumnsMap() {
        Map<String, StudyTableColumn> columnMap = new HashMap<>();
        columns.forEach(column -> columnMap.put(column.getName(), column));
        return Collections.unmodifiableMap(columnMap);
    }

}
