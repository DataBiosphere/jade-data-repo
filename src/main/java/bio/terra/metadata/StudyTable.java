package bio.terra.metadata;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableModel;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StudyTable {

    private String name;
    private Map<String, StudyTableColumn> columns;
    private UUID id;

    public StudyTable(TableModel tableModel) {
        this.name = tableModel.getName();
        this.columns = new HashMap<>();
        for (ColumnModel columnModel : tableModel.getColumns()) {
            this.columns.put(columnModel.getName(), new StudyTableColumn(columnModel));
        }
    }

    // Constructor for assembling test studies. Maybe useful for DAO.
    public StudyTable(String name, Map<String, StudyTableColumn> studyTableColumns) {
        this.name = name;
        this.columns = studyTableColumns;
    }

    public String getName() {
        return name;
    }

    public Collection<StudyTableColumn> getColumns() {
        return Collections.unmodifiableCollection(columns.values());
    }

    Map<String, StudyTableColumn> getColumnsMap() {
        return Collections.unmodifiableMap(columns);
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID tableId) { this.id = tableId; }
}
