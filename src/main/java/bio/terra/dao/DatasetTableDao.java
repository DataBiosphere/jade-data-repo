package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Column;
import bio.terra.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Repository
public class DatasetTableDao extends TableDaoBase {

    private static final Logger logger = LoggerFactory.getLogger(DatasetTableDao.class);

    private static final String sqlInsertTable = "INSERT INTO dataset_table " +
        "(name, dataset_id, primary_key) VALUES (:name, :dataset_id, :primary_key)";

    @Autowired
    public DatasetTableDao(DataRepoJdbcConfiguration dataRepoJdbcConfiguration) {
        super(dataRepoJdbcConfiguration, "dataset_table", "dataset_column", "dataset_id");
    }

    @Override
    protected UUID createTable(UUID parentId, Table table) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(jdbcDataSource);
        MapSqlParameterSource params = new MapSqlParameterSource();
        DaoKeyHolder keyHolder = new DaoKeyHolder();

        logger.info(parentId.toString());
        params.addValue("name", table.getName());
        params.addValue("dataset_id", parentId);

        List<String> naturalKeyStringList = Optional.ofNullable(table.getPrimaryKey())
            .orElse(Collections.emptyList())
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
        return keyHolder.getId();
    }

    @Override
    protected Table retrieveTable(ResultSet rs) throws SQLException {
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
    }
}
