package bio.terra.metadata;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DatasetMapTable {
    private UUID id;
    private StudyTable fromTable;
    private Table toTable;
    private List<DatasetMapColumn> datasetMapColumns = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public DatasetMapTable id(UUID id) {
        this.id = id;
        return this;
    }

    public StudyTable getFromTable() {
        return fromTable;
    }

    public DatasetMapTable fromTable(StudyTable fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Table getToTable() {
        return toTable;
    }

    public DatasetMapTable toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public List<DatasetMapColumn> getDatasetMapColumns() {
        return datasetMapColumns;
    }

    public DatasetMapTable datasetMapColumns(List<DatasetMapColumn> datasetMapColumns) {
        this.datasetMapColumns = datasetMapColumns;
        return this;
    }
}
