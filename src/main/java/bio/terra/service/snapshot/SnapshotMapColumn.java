package bio.terra.service.snapshot;

import bio.terra.common.Column;

import java.util.UUID;

public class SnapshotMapColumn {
    private UUID id;
    private Column fromColumn;
    private Column toColumn;

    public UUID getId() {
        return id;
    }

    public SnapshotMapColumn id(UUID id) {
        this.id = id;
        return this;
    }

    public Column getFromColumn() {
        return fromColumn;
    }

    public SnapshotMapColumn fromColumn(Column fromColumn) {
        this.fromColumn = fromColumn;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public SnapshotMapColumn toColumn(Column toColumn) {
        this.toColumn = toColumn;
        return this;
    }
}
