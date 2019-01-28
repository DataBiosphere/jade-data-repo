package bio.terra.metadata;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableModel;

import java.util.*;

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
    public StudyTable(String name, List<StudyTableColumn> studyTableColumns) {
        this.name = name;
        this.columns = studyTableColumns;
    }

    public String getName() {
        return name;
    }

//    public void setName(String name) {
//        this.name = name;
//    }

    // determine whether we need this method
    public Map<String, StudyTableColumn> getColumns() {
        return Collections.unmodifiableMap(columns);
    }

//    public void setColumns(List<StudyTableColumn> columns) {
//        this.columns = columns;
//    }


    public void setId(UUID tableId) { this.id = tableId; }
}
