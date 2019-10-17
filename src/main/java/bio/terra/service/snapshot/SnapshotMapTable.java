package bio.terra.service.snapshot;

import bio.terra.common.Table;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SnapshotMapTable {
    private UUID id;
    private Table fromTable;
    private Table toTable;
    private List<SnapshotMapColumn> snapshotMapColumns = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public SnapshotMapTable id(UUID id) {
        this.id = id;
        return this;
    }

    public Table getFromTable() {
        return fromTable;
    }

    public SnapshotMapTable fromTable(Table fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Table getToTable() {
        return toTable;
    }

    public SnapshotMapTable toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public List<SnapshotMapColumn> getSnapshotMapColumns() {
        return snapshotMapColumns;
    }

    public SnapshotMapTable snapshotMapColumns(List<SnapshotMapColumn> snapshotMapColumns) {
        this.snapshotMapColumns = snapshotMapColumns;
        return this;
    }
}
