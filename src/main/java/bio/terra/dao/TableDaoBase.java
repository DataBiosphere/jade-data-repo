package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Column;
import bio.terra.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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

public class TableDaoBase {
    private static final Logger logger = LoggerFactory.getLogger(TableDaoBase.class);

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String sqlInsertTable;
    private final String sqlInsertColumn;
    private final String sqlSelectTable;
    private final String sqlSelectColumn;
    private final Connection connection;

    public TableDaoBase(DataRepoJdbcConfiguration jdbcConfiguration,
                        String tableTableName,
                        String columnTableName,
                        String parentIdColumnName)  {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
        try {
            this.connection = jdbcConfiguration.getDataSource().getConnection();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        this.sqlInsertColumn = "INSERT INTO " + columnTableName +
            " (table_id, name, type, array_of) VALUES (:table_id, :name, :type, :arrayOf)";
        this.sqlInsertTable = "INSERT INTO " + tableTableName + " (name, " +
            parentIdColumnName + ", primary_key) VALUES (:name, :parent_id, :primary_key)";
        this.sqlSelectTable = "SELECT id, name, primary_key FROM " + tableTableName + " WHERE " +
            parentIdColumnName + " = :parentId";
        this.sqlSelectColumn = "SELECT id, name, type, array_of FROM " + columnTableName + " WHERE table_id = :tableId";
    }

    // Assumes transaction propagation from parent's create
    public void createTables(UUID parentId, List<Table> tableList) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("parent_id", parentId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (Table table : tableList) {
            params.addValue("name", table.getName());
            List<String> naturalKeyStringList = table.getPrimaryKey()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());
            try {
                params.addValue("primary_key", DaoUtils.createSqlStringArray(connection,
                    naturalKeyStringList));
            } catch (SQLException e) {
                logger.error(e.getMessage());
                throw new IllegalArgumentException(e.getMessage(), e);
            }

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
            params.addValue("arrayOf", column.isArrayOf());
            jdbcTemplate.update(sqlInsertColumn, params, keyHolder);
            UUID columnId = keyHolder.getId();
            column.id(columnId);
        }
    }

    // also retrieves columns
    public List<Table> retrieveTables(UUID parentId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("parentId", parentId);

        List<Table> tables = jdbcTemplate.query(sqlSelectTable, params, (rs, rowNum) -> {
            Table table = new Table()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"));
            List<String> primaryKey = DaoUtils.getStringList(rs, "primary_key");
            List<Column> columns = retrieveColumns(table);
            Map<String, Column> columnMap = columns
                .stream()
                .collect(Collectors.toMap(Column::getName, Function.identity()));
            if (primaryKey != null) {
                List<Column> naturalKeyColumns = primaryKey
                    .stream()
                    .map(columnMap::get)
                    .collect(Collectors.toList());
                table.primaryKey(naturalKeyColumns);
            }

            return table.columns(columns);
        });


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
                    .type(rs.getString("type"))
                    .arrayOf(rs.getBoolean("array_of")));
        return columns;
    }
}
