package bio.terra.service.dataset;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.Table;
import bio.terra.model.TableDataType;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DatasetTableDao {

  private static final Logger logger = LoggerFactory.getLogger(DatasetTableDao.class);

  private static final String sqlInsertTable =
      "INSERT INTO dataset_table "
          + "(name, raw_table_name, soft_delete_table_name, row_metadata_table_name, dataset_id, primary_key, bigquery_partition_config) "
          + "VALUES (:name, :raw_table_name, :soft_delete_table_name, :row_metadata_table_name, :dataset_id, :primary_key, "
          + "cast(:bigquery_partition_config AS jsonb))";
  private static final String sqlInsertColumn =
      "INSERT INTO dataset_column "
          + "(table_id, name, type, array_of, required, ordinal) "
          + "VALUES (:table_id, :name, :type, :array_of, :required, :ordinal)";
  private static final String sqlSelectTable =
      "SELECT id, name, raw_table_name, soft_delete_table_name, row_metadata_table_name, primary_key, bigquery_partition_config::text, "
          + "(bigquery_partition_config->>'version')::bigint AS bigquery_partition_config_version "
          + "FROM dataset_table WHERE dataset_id = :dataset_id";
  private static final String sqlSelectColumn =
      "SELECT id, name, type, array_of, required "
          + "FROM dataset_column "
          + "WHERE table_id = :table_id "
          + "ORDER BY ordinal";

  private static final String sqlDeleteTable =
      "DELETE FROM dataset_table WHERE id = :table_id AND dataset_id = :dataset_id";

  private static final String sqlDeleteColumn =
      "DELETE FROM dataset_column WHERE id = :column_id AND table_id = :table_id";

  private static final String sqlGetMaxColumnOrdinal =
      "SELECT MAX(ordinal) FROM dataset_column WHERE table_id = :table_id";

  private final DataSource jdbcDataSource;
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  @Autowired
  public DatasetTableDao(
      DataRepoJdbcConfiguration jdbcConfiguration,
      NamedParameterJdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper) {
    this.jdbcDataSource = jdbcConfiguration.getDataSource();
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  // Assumes transaction propagation from parent's create
  public void createTables(UUID parentId, List<DatasetTable> tableList) throws IOException {
    MapSqlParameterSource params = new MapSqlParameterSource();
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    params.addValue("dataset_id", parentId);

    for (DatasetTable table : tableList) {
      params.addValue("name", table.getName());
      params.addValue("raw_table_name", table.getRawTableName());
      params.addValue("soft_delete_table_name", table.getSoftDeleteTableName());
      params.addValue("row_metadata_table_name", table.getRowMetadataTableName());
      params.addValue(
          "bigquery_partition_config",
          objectMapper.writeValueAsString(table.getBigQueryPartitionConfig()));

      List<String> naturalKeyStringList =
          table.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList());
      try (Connection connection = jdbcDataSource.getConnection()) {
        params.addValue(
            "primary_key", DaoUtils.createSqlStringArray(connection, naturalKeyStringList));
      } catch (SQLException e) {
        logger.error("Failed to convert primary key list to SQL array", e);
        throw new IllegalArgumentException("Failed to convert primary key list to SQL array", e);
      }

      jdbcTemplate.update(sqlInsertTable, params, keyHolder);

      UUID tableId = keyHolder.getId();
      table.id(tableId);
      createColumnsNewTable(tableId, table.getColumns());
    }
  }

  public void removeTables(UUID parentId, List<String> tableNames) {
    List<DatasetTable> tablesToDelete =
        retrieveTables(parentId).stream()
            .filter(dt -> tableNames.contains(dt.getName()))
            .collect(Collectors.toList());

    for (DatasetTable tableToDelete : tablesToDelete) {
      removeColumns(parentId, tableToDelete.getId(), tableToDelete.getColumns());
      MapSqlParameterSource params = new MapSqlParameterSource();
      params.addValue("table_id", tableToDelete.getId());
      params.addValue("dataset_id", parentId);
      jdbcTemplate.update(sqlDeleteTable, params);
    }
  }

  private void createColumnsNewTable(UUID tableId, Collection<Column> columns) {
    createColumns(tableId, columns, 0);
  }

  public void createColumnsExistingTable(UUID tableId, Collection<Column> columns) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("table_id", tableId);
    Integer maxOrdinal = jdbcTemplate.queryForObject(sqlGetMaxColumnOrdinal, params, Integer.class);
    // Need to requireNonNull or else SpotBugs complains
    createColumns(tableId, columns, Objects.requireNonNullElse(maxOrdinal, 0) + 1);
  }

  private void createColumns(UUID tableId, Collection<Column> columns, int ordinal) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("table_id", tableId);
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    for (Column column : columns) {
      params.addValue("name", column.getName());
      params.addValue("type", column.getType().toString());
      params.addValue("array_of", column.isArrayOf());
      params.addValue("required", column.isRequired());
      params.addValue("ordinal", ordinal++);
      jdbcTemplate.update(sqlInsertColumn, params, keyHolder);
      UUID columnId = keyHolder.getId();
      column.id(columnId);
    }
  }

  public void removeColumns(Table table, Collection<Column> columns) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("table_id", table.getId());

    List<Column> existingColumns = retrieveColumns(table);
    Collection<Column> columnsToDelete = CollectionUtils.intersection(existingColumns, columns);
    for (Column column : columnsToDelete) {
      params.addValue("column_id", column.getId());
      jdbcTemplate.update(sqlDeleteColumn, params);
    }
  }

  private void removeColumns(UUID datasetId, UUID tableId, Collection<Column> columns) {
    removeColumns(
        retrieveTables(datasetId).stream()
            .filter(dt -> dt.getId().equals(tableId))
            .findFirst()
            .orElseThrow(),
        columns);
  }

  public List<DatasetTable> retrieveTables(UUID parentId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", parentId);
    return jdbcTemplate.query(
        sqlSelectTable,
        params,
        (rs, rowNum) -> {
          DatasetTable table =
              new DatasetTable()
                  .id(rs.getObject("id", UUID.class))
                  .name(rs.getString("name"))
                  .rawTableName(rs.getString("raw_table_name"))
                  .softDeleteTableName(rs.getString("soft_delete_table_name"))
                  .rowMetadataTableName(rs.getString("row_metadata_table_name"));

          List<Column> columns = retrieveColumns(table);
          table.columns(columns);

          Map<String, Column> columnMap =
              columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));

          List<String> primaryKey = DaoUtils.getStringList(rs, "primary_key");
          List<Column> naturalKeyColumns =
              primaryKey.stream().map(columnMap::get).collect(Collectors.toList());
          table.primaryKey(naturalKeyColumns);

          long bqPartitionVersion = rs.getLong("bigquery_partition_config_version");
          String bqPartitionConfig = rs.getString("bigquery_partition_config");
          if (bqPartitionVersion == 1) {
            try {
              table.bigQueryPartitionConfig(
                  objectMapper.readValue(bqPartitionConfig, BigQueryPartitionConfigV1.class));
            } catch (Exception ex) {
              throw new CorruptMetadataException("Malformed BigQuery partition config", ex);
            }
          } else {
            throw new CorruptMetadataException(
                "Unknown BigQuery partition config version: " + bqPartitionVersion);
          }

          return table;
        });
  }

  List<String> retrieveColumnNames(Table table, boolean includeDataRepoRowId) {
    List<String> columns = new ArrayList<>();
    if (includeDataRepoRowId) {
      columns.add(PDAO_ROW_ID_COLUMN);
    }
    columns.addAll(retrieveColumns(table).stream().map(Column::getName).toList());
    return columns;
  }

  List<Column> retrieveColumns(Table table) {
    return jdbcTemplate.query(
        sqlSelectColumn,
        new MapSqlParameterSource().addValue("table_id", table.getId()),
        (rs, rowNum) ->
            new Column()
                .id(rs.getObject("id", UUID.class))
                .table(table)
                .name(rs.getString("name"))
                .type(TableDataType.fromValue(rs.getString("type")))
                .arrayOf(rs.getBoolean("array_of"))
                .required(rs.getBoolean("required")));
  }
}
