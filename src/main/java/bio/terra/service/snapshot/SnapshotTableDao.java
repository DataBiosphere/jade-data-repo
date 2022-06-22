package bio.terra.service.snapshot;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.Table;
import bio.terra.model.TableDataType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotTableDao {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotTableDao.class);

  private static final String sqlInsertTable =
      "INSERT INTO snapshot_table "
          + "(name, parent_id, primary_key) "
          + "VALUES (:name, :parent_id, :primary_key)";
  private static final String sqlInsertColumn =
      "INSERT INTO snapshot_column "
          + "(table_id, name, type, array_of, required, ordinal) "
          + "VALUES (:table_id, :name, :type, :array_of, :required, :ordinal)";
  private static final String sqlSelectTable =
      "SELECT id, name, row_count, primary_key FROM snapshot_table WHERE parent_id = :parent_id";
  private static final String sqlSelectColumn =
      "SELECT id, name, type, array_of, required "
          + "FROM snapshot_column "
          + "WHERE table_id = :table_id "
          + "ORDER BY ordinal";

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final DataSource jdbcDataSource;

  @Autowired
  public SnapshotTableDao(
      DataRepoJdbcConfiguration jdbcConfiguration, NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
    this.jdbcDataSource = jdbcConfiguration.getDataSource();
  }

  public void createTables(UUID parentId, List<SnapshotTable> tableList) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    params.addValue("parent_id", parentId);

    for (SnapshotTable table : tableList) {
      params.addValue("name", table.getName());
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
      createColumns(tableId, table.getColumns());
    }
  }

  private void createColumns(UUID tableId, Collection<Column> columns) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("table_id", tableId);
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    int ordinal = 0;
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

  public List<SnapshotTable> retrieveTables(UUID parentId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("parent_id", parentId);
    return jdbcTemplate.query(
        sqlSelectTable,
        params,
        (rs, rowNum) -> {
          SnapshotTable table =
              new SnapshotTable()
                  .id(rs.getObject("id", UUID.class))
                  .name(rs.getString("name"))
                  .rowCount(rs.getLong("row_count"));
          List<Column> columns = retrieveColumns(table);
          table.columns(columns);

          Map<String, Column> columnMap =
              columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));

          List<String> primaryKey = DaoUtils.getStringList(rs, "primary_key");
          List<Column> naturalKeyColumns =
              primaryKey.stream().map(columnMap::get).collect(Collectors.toList());
          table.primaryKey(naturalKeyColumns);

          return table;
        });
  }

  public List<Column> retrieveColumns(Table table) {
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
