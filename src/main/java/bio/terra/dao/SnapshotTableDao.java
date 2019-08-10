package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Column;
import bio.terra.metadata.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Repository
public class SnapshotTableDao extends TableDaoBase {

    private static final String sqlInsertTable = "INSERT INTO snapshot_table " +
        "(name, parent_id) VALUES (:name, :parent_id)";

    @Autowired
    public SnapshotTableDao(DataRepoJdbcConfiguration dataRepoJdbcConfiguration) {
        super(dataRepoJdbcConfiguration, "snapshot_table", "snapshot_column", "parent_id");
    }

    @Override
    protected UUID createTable(UUID parentId, Table table) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(jdbcDataSource);
        MapSqlParameterSource params = new MapSqlParameterSource();
        DaoKeyHolder keyHolder = new DaoKeyHolder();

        params.addValue("name", table.getName());
        params.addValue("parent_id", parentId);

        jdbcTemplate.update(sqlInsertTable, params, keyHolder);
        return keyHolder.getId();
    }

    @Override
    protected Table retrieveTable(ResultSet rs) throws SQLException {
        Table table = new Table()
            .id(rs.getObject("id", UUID.class))
            .name(rs.getString("name"));

        List<Column> columns = retrieveColumns(table);
        return table.columns(columns);
    }
}
