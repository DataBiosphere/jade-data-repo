package bio.terra.service.snapshot;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
      """
  INSERT INTO snapshot_table
  (name, parent_id, primary_key)
  VALUES (:name, :parent_id, :primary_key)
  """;

  private static final String sqlInsertColumn =
      """
  INSERT INTO snapshot_column
  (table_id, name, type, array_of, required, ordinal)
  VALUES (:table_id, :name, :type, :array_of, :required, :ordinal)
  """;

  private static final String sqlSelectTable =
      """
  SELECT t.id table_id, t.name table_name, t.row_count table_row_count, t.primary_key table_primary_key,
         c.id column_id, c.name column_name, c.type column_type, c.array_of column_array_of, c.required column_required
  FROM snapshot_table t
     INNER JOIN snapshot_column c ON t.id = c.table_id
  WHERE parent_id = :snapshot_id
  ORDER BY t.ctid, c.ordinal
  """;

  private static final String sqlSelectColumn =
      """
  SELECT id column_id, name column_name, type column_type, array_of column_array_of, required column_required
  FROM snapshot_column
  WHERE table_id = :table_id
  ORDER BY ordinal
  """;

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

  public List<SnapshotTable> retrieveTables(UUID snapshotId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("snapshot_id", snapshotId);
    // A tree map is used here to preserve the order from the SQL query
    Map<UUID, SnapshotTable> tableMap = new TreeMap<>();
    // Order is not important here, so we use a HashMap
    Map<UUID, List<String>> primaryKeyMap = new HashMap<>();
    jdbcTemplate.query(
        sqlSelectTable,
        params,
        (rs, rowNum) -> {
          UUID tableId = rs.getObject("table_id", UUID.class);
          List<String> primaryKeyColumns =
              primaryKeyMap.computeIfAbsent(
                  tableId,
                  (id) -> {
                    try {
                      return DaoUtils.getStringList(rs, "table_primary_key");
                    } catch (SQLException e) {
                      throw new IllegalArgumentException(
                          "Failed to extract primary key information", e);
                    }
                  });
          SnapshotTable table =
              tableMap.computeIfAbsent(
                  tableId,
                  (id) -> {
                    try {
                      return new SnapshotTable()
                          .id(id)
                          .name(rs.getString("table_name"))
                          .rowCount(rs.getLong("table_row_count"))
                          .columns(new ArrayList<>())
                          .primaryKey(
                              // Null values will all be replaced.
                              // This is safe since validation is done on table creation
                              // and columns cannot be removed.
                              new ArrayList<>(
                                  IntStream.range(0, primaryKeyColumns.size())
                                      .mapToObj(i -> (Column) null)
                                      .toList()));

                    } catch (SQLException e) {
                      throw new IllegalArgumentException("Failed to extract table information", e);
                    }
                  });

          // Add column to table
          Column column = new DaoUtils.TableColumnMapper(table).mapRow(rs, rowNum);
          table.getColumns().add(column);

          int pkIndex = primaryKeyColumns.indexOf(column.getName());
          if (pkIndex != -1) {
            table.getPrimaryKey().set(pkIndex, column);
          }

          return table;
        });
    return List.copyOf(tableMap.values());
  }

  public List<Column> retrieveColumns(Table table) {
    return jdbcTemplate.query(
        sqlSelectColumn,
        new MapSqlParameterSource().addValue("table_id", table.getId()),
        new DaoUtils.TableColumnMapper(table));
  }
}
