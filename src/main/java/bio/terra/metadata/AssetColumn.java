package bio.terra.metadata;

import java.util.UUID;

public class AssetColumn {
    private UUID id;
    private StudyTableColumn studyColumn;

    public AssetColumn() {}

    public UUID getId() { return id; }
    public AssetColumn setId(UUID id) { this.id = id; return this; }

    public StudyTableColumn getStudyColumn() {
        return studyColumn;
    }
    public AssetColumn setStudyColumn(StudyTableColumn studyColumn) {
        this.studyColumn = studyColumn;
        return this;
    }
}
