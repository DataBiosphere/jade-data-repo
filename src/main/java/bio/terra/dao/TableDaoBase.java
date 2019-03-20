package bio.terra.dao;

import bio.terra.metadata.Column;
import bio.terra.metadata.Table;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

// Base class for study and dataset daos.
// Assumes the Table table is:
//  id uuid         - unique id of the table
//  parent_id uuid  - id of the parent (study or dataset)
//  name varchar    - name of the table
// And the Column table is:
//  id uuid         - unique id of the column
//  table_id uuid   - id of the parent table
//  name varchar    - name of the column
//  type varchar    - datatype of the column

public class TableDaoBase {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String sqlInsertTable;
    private final String sqlInsertColumn;
    private final String sqlSelectTable;
    private final String sqlSelectColumn;

    public TableDaoBase(NamedParameterJdbcTemplate jdbcTemplate,
                        String tableTableName,
                        String columnTableName,
                        String parentIdColumnName) {
        this.jdbcTemplate = jdbcTemplate;
        this.sqlInsertColumn = "INSERT INTO " + columnTableName +
            " (table_id, name, type) VALUES (:table_id, :name, :type)";
        this.sqlInsertTable = "INSERT INTO " + tableTableName + " (name, " +
            parentIdColumnName + ") VALUES (:name, :parent_id)";
        this.sqlSelectTable = "SELECT id, name FROM " + tableTableName + " WHERE " +
            parentIdColumnName + " = :parentId";
        this.sqlSelectColumn = "SELECT id, name, type FROM " + columnTableName + " WHERE table_id = :tableId";
    }

    // Assumes transaction propagation from parent's create
    public void createTables(UUID parentId, List<Table> tableList) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("parent_id", parentId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (Table table : tableList) {
            params.addValue("name", table.getName());
            jdbcTemplate.update(sqlInsertTable, params, keyHolder);
            UUID tableId = keyHolder.getId();
            table.id(tableId);
            createColumns(tableId, table.getColumns());
        }
    }

    protected void createColumns(UUID tableId, Collection<Column> columns) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("table_id", tableId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (Column column : columns) {
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            jdbcTemplate.update(sqlInsertColumn, params, keyHolder);
            UUID columnId = keyHolder.getId();
            column.id(columnId);
        }
    }

    // also retrieves columns
    public List<Table> retrieveTables(UUID parentId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("parentId", parentId);


        List<Table> tables = jdbcTemplate.query(sqlSelectTable, params, (rs, rowNum) ->
                new Table()
                        .id(rs.getObject("id", UUID.class))
                        .name(rs.getString("name")));
        tables.forEach(table -> table.columns(retrieveColumns(table)));
        return tables;
    }

    private List<Column> retrieveColumns(Table table) {
        List<Column> columns = jdbcTemplate.query(
                sqlSelectColumn,
                new MapSqlParameterSource().addValue("tableId", table.getId()), (rs, rowNum) ->
                        new Column()
                                .id(rs.getObject("id", UUID.class))
                                .table(table)
                                .name(rs.getString("name"))
                                .type(rs.getString("type")));
        return columns;
    }
}
