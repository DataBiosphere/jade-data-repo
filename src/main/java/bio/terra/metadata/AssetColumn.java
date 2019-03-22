package bio.terra.metadata;

import java.util.UUID;

public class AssetColumn {
    private UUID id;
    private Table studyTable;
    private Column studyColumn;

    public UUID getId() {
        return id;
    }

    public AssetColumn id(UUID id) {
        this.id = id;
        return this;
    }

    public Column getStudyColumn() {
        return studyColumn;
    }

    public AssetColumn studyColumn(Column studyColumn) {
        this.studyColumn = studyColumn;
        return this;
    }

    public Table getTable() {
        return studyTable;
    }

    public AssetColumn studyTable(Table studyTable) {
        this.studyTable = studyTable;
        return this;
    }
}
