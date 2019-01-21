package bio.terra.metadata;

import bio.terra.model.ColumnModel;

public class StudyTableColumn {
    private String name;
    private String type;

    public StudyTableColumn(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public StudyTableColumn(ColumnModel columnModel) {
        this.name = columnModel.getName();
        this.type = columnModel.getDatatype();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
