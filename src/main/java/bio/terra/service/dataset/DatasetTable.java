package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.LogPrintable;
import bio.terra.common.Table;
import com.google.cloud.bigquery.FieldValueList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Specific class for modeling dataset tables.
 *
 * <p>Includes extra info to capture: 1. Primary keys 2. Names of helper tables used when building
 * "live views" 3. Configuration for partitioning the table in BigQuery
 */
public class DatasetTable implements Table, LogPrintable {

  private UUID id;
  private String name;
  private String rawTableName;
  private String softDeleteTableName;
  private String rowMetadataTableName;
  private List<Column> columns = Collections.emptyList();
  private List<Column> primaryKey = Collections.emptyList();
  private BigQueryPartitionConfigV1 bqPartitionConfig;
  private Long rowCount;

  @Override
  public UUID getId() {
    return id;
  }

  public DatasetTable id(UUID id) {
    this.id = id;
    return this;
  }

  @Override
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

  public String getRowMetadataTableName() {
    return rowMetadataTableName;
  }

  public DatasetTable rowMetadataTableName(String name) {
    this.rowMetadataTableName = name;
    return this;
  }

  @Override
  public List<Column> getColumns() {
    return columns;
  }

  public DatasetTable columns(List<Column> columns) {
    this.columns = columns;
    return this;
  }

  public Column getColumnByName(String columnName) {
    return this.columns.stream()
        .filter(column -> column.getName().equals(columnName))
        .findFirst()
        .orElseThrow();
  }

  @Override
  public List<Column> getPrimaryKey() {
    return primaryKey;
  }

  public DatasetTable primaryKey(List<Column> primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }

  /**
   * @param column with name as key
   * @param row expected to contain a value associated with `column`
   * @return value associated with `column`
   */
  private String columnValueFromRow(Column column, FieldValueList row) {
    if (!row.hasSchema()) {
      return "unknown";
    }
    try {
      return row.get(column.getName()).getStringValue();
    } catch (IllegalArgumentException e) {
      return "unknown";
    } catch (NullPointerException e) {
      return "null";
    }
  }

  /**
   * @param row expected to contain values associated with the table's primary key
   * @return a string representation of the table's primary key and its values
   */
  public String primaryKeyToString(FieldValueList row) {
    if (primaryKey == null || primaryKey.isEmpty()) {
      return "undefined";
    } else {
      return primaryKey.stream()
          .map(c -> c.getName() + "=" + columnValueFromRow(c, row))
          .collect(Collectors.joining(","));
    }
  }

  public BigQueryPartitionConfigV1 getBigQueryPartitionConfig() {
    return bqPartitionConfig;
  }

  public DatasetTable bigQueryPartitionConfig(BigQueryPartitionConfigV1 config) {
    this.bqPartitionConfig = config;
    return this;
  }

  @Override
  public Long getRowCount() {
    return rowCount;
  }

  public DatasetTable setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  @Override
  public String toLogString() {
    return String.format("%s (%s)", this.getName(), this.getId());
  }

  public List<String> getBigQueryTableNames() {
    return List.of(
        this.name, this.rawTableName, this.softDeleteTableName, this.rowMetadataTableName);
  }
}
