package bio.terra.metadata;

import java.util.UUID;

public class StudyTableColumn {
    private String name;
    private String type;
    private UUID id;
    private volatile StudyTable inTable;

    public StudyTableColumn() {}

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

    public StudyTable getInTable() { return inTable; }
    public StudyTableColumn setInTable(StudyTable inTable) { this.inTable = inTable; return this; }

}
