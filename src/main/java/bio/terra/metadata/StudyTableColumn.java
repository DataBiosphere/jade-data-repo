package bio.terra.metadata;

import bio.terra.model.ColumnModel;

import java.util.UUID;

public class StudyTableColumn {
    private String name;
    private String type;
    private UUID id;

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

    public UUID getId() { return id; }

    public void setId(UUID id) {
        this.id = id;
    }
}
