package bio.terra.metadata;

import bio.terra.model.ColumnModel;

import java.util.UUID;

public class StudyTableColumn {
    private String name;
    private String type;
    private UUID id;

    public StudyTableColumn() {}

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
    public StudyTableColumn setName(String name) { this.name = name; return this; }

    public String getType() {
        return type;
    }
    public StudyTableColumn setType(String type) { this.type = type; return this; }

    public UUID getId() { return id; }
    public StudyTableColumn setId(UUID id) { this.id = id; return this; }
}
