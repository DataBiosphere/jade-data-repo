package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.Table;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Specific class for modeling dataset tables. Includes primary key info.
 */
public class DatasetTable implements Table {

    private UUID id;
    private String name;
    private List<Column> columns = Collections.emptyList();
    private List<Column> primaryKey = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public DatasetTable id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DatasetTable name(String name) {
        this.name = name;
        return this;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public DatasetTable columns(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public List<Column> getPrimaryKey() {
        return primaryKey;
    }

    public DatasetTable primaryKey(List<Column> primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }
}
