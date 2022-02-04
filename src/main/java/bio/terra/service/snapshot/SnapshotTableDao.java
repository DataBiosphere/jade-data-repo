package bio.terra.service.snapshot;

import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Table;
import bio.terra.model.TableDataType;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotTableDao {

  private static final String sqlInsertTable =
      "INSERT INTO snapshot_table " + "(name, parent_id) VALUES (:name, :parent_id)";
  private static final String sqlInsertColumn =
      "INSERT INTO snapshot_column "
          + "(table_id, name, type, array_of, ordinal) "
          + "VALUES (:table_id, :name, :type, :array_of, :ordinal)";
  private static final String sqlSelectTable =
      "SELECT id, name, row_count FROM snapshot_table WHERE parent_id = :parent_id";
  private static final String sqlSelectColumn =
      "SELECT id, name, type, array_of "
          + "FROM snapshot_column "
          + "WHERE table_id = :table_id "
          + "ORDER BY ordinal";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SnapshotTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void createTables(UUID parentId, List<SnapshotTable> tableList) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    params.addValue("parent_id", parentId);

    for (SnapshotTable table : tableList) {
      params.addValue("name", table.getName());
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
          return table.columns(columns);
        });
  }

  private List<Column> retrieveColumns(Table table) {
    return jdbcTemplate.query(
        sqlSelectColumn,
        new MapSqlParameterSource().addValue("table_id", table.getId()),
        (rs, rowNum) ->
            new Column()
                .id(rs.getObject("id", UUID.class))
                .table(table)
                .name(rs.getString("name"))
                .type(TableDataType.fromValue(rs.getString("type")))
                .arrayOf(rs.getBoolean("array_of")));
  }
}
