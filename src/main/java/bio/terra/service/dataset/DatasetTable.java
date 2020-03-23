package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.Table;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Specific class for modeling dataset tables.
 *
 * Includes extra info to capture:
 *   1. Primary keys
 *   2. Names of helper tables used when building "live views"
 *   3. Configuration for partitioning the table in BigQuery
 */
public class DatasetTable implements Table {

    private UUID id;
    private String name;
    private String rawTableName;
    private String softDeleteTableName;
    private List<Column> columns = Collections.emptyList();
    private List<Column> primaryKey = Collections.emptyList();
    private PartitionMode partitionMode;

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

    public String getRawTableName() {
        return rawTableName;
    }

    public DatasetTable rawTableName(String name) {
        this.rawTableName = name;
        return this;
    }

    public String getSoftDeleteTableName() {
        return softDeleteTableName;
    }

    public DatasetTable softDeleteTableName(String name) {
        this.softDeleteTableName = name;
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

    public PartitionMode getPartitionMode() {
        return partitionMode;
    }

    public DatasetTable partitionMode(PartitionMode mode) {
        this.partitionMode = mode;
        return this;
    }
}
