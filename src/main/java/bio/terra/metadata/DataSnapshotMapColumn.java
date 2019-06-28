package bio.terra.metadata;

import java.util.UUID;

public class DataSnapshotMapColumn {
    private UUID id;
    private Column fromColumn;
    private Column toColumn;

    public UUID getId() {
        return id;
    }

    public DataSnapshotMapColumn id(UUID id) {
        this.id = id;
        return this;
    }

    public Column getFromColumn() {
        return fromColumn;
    }

    public DataSnapshotMapColumn fromColumn(Column fromColumn) {
        this.fromColumn = fromColumn;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public DataSnapshotMapColumn toColumn(Column toColumn) {
        this.toColumn = toColumn;
        return this;
    }
}
