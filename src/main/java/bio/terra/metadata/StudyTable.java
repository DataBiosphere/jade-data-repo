package bio.terra.metadata;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StudyTable {

    private String name;
    private List<StudyTableColumn> columns;
    private UUID id;

    public StudyTable() {}

    public StudyTable(TableModel tableModel) {
        this.name = tableModel.getName();
        this.columns = new ArrayList<>();
        for (ColumnModel columnModel : tableModel.getColumns()) {
            this.columns.add(new StudyTableColumn(columnModel));
        }
    }

    // Constructor for assembling test studies. Maybe useful for DAO.
    public StudyTable(String name, List<StudyTableColumn> studyTableColumns) {
        this.name = name;
        this.columns = studyTableColumns;
    }

    public UUID getId() { return id; }
    public StudyTable setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public StudyTable setName(String name) { this.name = name; return this; }

    protected Map<String, StudyTableColumn> getColumnsMap() {
        Map<String, StudyTableColumn> columnMap = new HashMap<>();
        columns.forEach(column -> columnMap.put(column.getName(), column));
        return Collections.unmodifiableMap(columnMap);
    }
    public StudyTable setColumns(List<StudyTableColumn> columns) { this.columns = columns; return this; }

    public Collection<StudyTableColumn> getColumns() {
        return Collections.unmodifiableCollection(columns);
    }

}
