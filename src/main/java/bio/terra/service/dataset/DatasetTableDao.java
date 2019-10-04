package bio.terra.service.dataset;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.Column;
import bio.terra.common.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Repository
public class DatasetTableDao {

    private static final Logger logger = LoggerFactory.getLogger(DatasetTableDao.class);

    private static final String sqlInsertTable = "INSERT INTO dataset_table " +
        "(name, dataset_id, primary_key) VALUES (:name, :dataset_id, :primary_key)";
    private static final String sqlInsertColumn = "INSERT INTO dataset_column " +
        "(table_id, name, type, array_of) VALUES (:table_id, :name, :type, :array_of)";
    private static final String sqlSelectTable = "SELECT id, name, primary_key FROM dataset_table " +
        "WHERE dataset_id = :dataset_id";
    private static final String sqlSelectColumn = "SELECT id, name, type, array_of FROM dataset_column " +
        "WHERE table_id = :table_id";

    private final DataSource jdbcDataSource;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DatasetTableDao(DataRepoJdbcConfiguration jdbcConfiguration, NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcDataSource = jdbcConfiguration.getDataSource();
        this.jdbcTemplate = jdbcTemplate;
    }

    // Assumes transaction propagation from parent's create
    public void createTables(UUID parentId, List<DatasetTable> tableList) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        params.addValue("dataset_id", parentId);

        for (DatasetTable table : tableList) {
            params.addValue("name", table.getName());

            List<String> naturalKeyStringList = table.getPrimaryKey()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());
            try (Connection connection = jdbcDataSource.getConnection()) {
                params.addValue("primary_key", DaoUtils.createSqlStringArray(connection, naturalKeyStringList));
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
        for (Column column : columns) {
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            params.addValue("array_of", column.isArrayOf());
            jdbcTemplate.update(sqlInsertColumn, params, keyHolder);
            UUID columnId = keyHolder.getId();
            column.id(columnId);
        }
    }

    public List<DatasetTable> retrieveTables(UUID parentId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", parentId);
        return jdbcTemplate.query(sqlSelectTable, params, (rs, rowNum) -> {
            DatasetTable table = new DatasetTable()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"));

            List<String> primaryKey = DaoUtils.getStringList(rs, "primary_key");
            List<Column> columns = retrieveColumns(table);
            Map<String, Column> columnMap = columns
                .stream()
                .collect(Collectors.toMap(Column::getName, Function.identity()));

            List<Column> naturalKeyColumns = primaryKey.stream()
                .map(columnMap::get)
                .collect(Collectors.toList());
            table.primaryKey(naturalKeyColumns);

            return table.columns(columns);
        });
    }

    private List<Column> retrieveColumns(Table table) {
        return jdbcTemplate.query(
            sqlSelectColumn,
            new MapSqlParameterSource().addValue("table_id", table.getId()), (rs, rowNum) ->
                new Column()
                    .id(rs.getObject("id", UUID.class))
                    .table(table)
                    .name(rs.getString("name"))
                    .type(rs.getString("type"))
                    .arrayOf(rs.getBoolean("array_of")));
    }
}
