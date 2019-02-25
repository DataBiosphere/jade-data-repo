package bio.terra.metadata;

import java.util.UUID;

public class AssetColumn {
    private UUID id;
    private StudyTable studyTable;
    private StudyTableColumn studyColumn;

    public UUID getId() {
        return id;
    }

    public AssetColumn id(UUID id) {
        this.id = id;
        return this;
    }

    public StudyTableColumn getStudyColumn() {
        return studyColumn;
    }

    public AssetColumn studyColumn(StudyTableColumn studyColumn) {
        this.studyColumn = studyColumn;
        return this;
    }

    public StudyTable getStudyTable() {
        return studyTable;
    }

    public AssetColumn studyTable(StudyTable studyTable) {
        this.studyTable = studyTable;
        return this;
    }
}
