package bio.terra.metadata;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableModel;

import java.util.ArrayList;
import java.util.List;

public class StudyTable {

    private String name;
    private List<StudyTableColumn> columns;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<StudyTableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<StudyTableColumn> columns) {
        this.columns = columns;
    }
}
