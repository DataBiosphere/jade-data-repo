package bio.terra.metadata;

import java.util.UUID;

public class AssetColumn {
    private UUID id;
    private StudyTableColumn studyColumn;

    public AssetColumn(StudyTableColumn studyColumn) {
        this.studyColumn = studyColumn;
    }

    public UUID getId() { return id; }

    public void setId(UUID id) {
        this.id = id;
    }

    public StudyTableColumn getStudyColumn() {
        return studyColumn;
    }
}
