package bio.terra.metadata;

import java.util.UUID;

public class DatasetMapColumn {
    private UUID id;
    private StudyTableColumn fromColumn;
    private Column toColumn;

    public UUID getId() {
        return id;
    }

    public DatasetMapColumn id(UUID id) {
        this.id = id;
        return this;
    }

    public StudyTableColumn getFromColumn() {
        return fromColumn;
    }

    public DatasetMapColumn fromColumn(StudyTableColumn fromColumn) {
        this.fromColumn = fromColumn;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public DatasetMapColumn toColumn(Column toColumn) {
        this.toColumn = toColumn;
        return this;
    }
}
