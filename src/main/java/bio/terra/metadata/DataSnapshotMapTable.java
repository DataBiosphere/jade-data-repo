package bio.terra.metadata;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DataSnapshotMapTable {
    private UUID id;
    private Table fromTable;
    private Table toTable;
    private List<DataSnapshotMapColumn> dataSnapshotMapColumns = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public DataSnapshotMapTable id(UUID id) {
        this.id = id;
        return this;
    }

    public Table getFromTable() {
        return fromTable;
    }

    public DataSnapshotMapTable fromTable(Table fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Table getToTable() {
        return toTable;
    }

    public DataSnapshotMapTable toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public List<DataSnapshotMapColumn> getDataSnapshotMapColumns() {
        return dataSnapshotMapColumns;
    }

    public DataSnapshotMapTable dataSnapshotMapColumns(List<DataSnapshotMapColumn> dataSnapshotMapColumns) {
        this.dataSnapshotMapColumns = dataSnapshotMapColumns;
        return this;
    }
}
