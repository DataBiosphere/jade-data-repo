package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Column;
import bio.terra.metadata.Table;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

// Base class for dataset and snapshot daos.
// Assumes the Table table is:
//  id uuid         - unique id of the table
//  parent_id uuid  - id of the parent (dataset or snapshot)
//  name varchar    - name of the table
// And the Column table is:
//  id uuid         - unique id of the column
//  table_id uuid   - id of the parent table
//  name varchar    - name of the column
//  type varchar    - datatype of the column

public abstract class TableDaoBase {

    private final PoolingDataSource<PoolableConnection> jdbcDataSource;
    private final String sqlInsertColumn;
    private final String sqlSelectTable;
    private final String sqlSelectColumn;

    public TableDaoBase(DataRepoJdbcConfiguration jdbcConfiguration,
                        String tableTableName,
                        String columnTableName,
                        String parentIdColumnName) {
        this.jdbcDataSource = jdbcConfiguration.getDataSource();
        this.sqlInsertColumn = "INSERT INTO " + columnTableName +
            " (table_id, name, type, array_of) VALUES (:table_id, :name, :type, :arrayOf)";
        this.sqlSelectTable = "SELECT * FROM " + tableTableName + " WHERE " + parentIdColumnName + " = :parentId";
        this.sqlSelectColumn = "SELECT id, name, type, array_of FROM " + columnTableName + " WHERE table_id = :tableId";
    }

    // Assumes transaction propagation from parent's create
    public void createTables(UUID parentId, List<Table> tableList) {
        for (Table table : tableList) {
            UUID tableId = createTable(jdbcDataSource, parentId, table);
            table.id(tableId);
            createColumns(tableId, table.getColumns());
        }
    }

    protected abstract UUID createTable(DataSource jdbcDataSource, UUID parentId, Table table);

    protected void createColumns(UUID tableId, Collection<Column> columns) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(jdbcDataSource);
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("table_id", tableId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (Column column : columns) {
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            params.addValue("arrayOf", column.isArrayOf());
            jdbcTemplate.update(sqlInsertColumn, params, keyHolder);
            UUID columnId = keyHolder.getId();
            column.id(columnId);
        }
    }

    protected abstract Table retrieveTable(ResultSet rs) throws SQLException;

    // also retrieves columns
    public List<Table> retrieveTables(UUID parentId) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(jdbcDataSource);
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("parentId", parentId);
        return jdbcTemplate.query(sqlSelectTable, params, (rs, rowNum) -> retrieveTable(rs));
    }

    protected List<Column> retrieveColumns(Table table) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(jdbcDataSource);
        List<Column> columns = jdbcTemplate.query(
            sqlSelectColumn,
            new MapSqlParameterSource().addValue("tableId", table.getId()), (rs, rowNum) ->
                new Column()
                    .id(rs.getObject("id", UUID.class))
                    .table(table)
                    .name(rs.getString("name"))
                    .type(rs.getString("type"))
                    .arrayOf(rs.getBoolean("array_of")));
        return columns;
    }
}
