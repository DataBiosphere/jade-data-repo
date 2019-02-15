package bio.terra.dao;

import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public class DatasetMapTableDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DatasetMapTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from DatasetDao

    public void createTables(UUID sourceId, List<DatasetMapTable> tableList) {
        String sql = "INSERT INTO dataset_map_table (source_id, from_table_id, to_table_id)" +
                "VALUES (:source_id, :from_table_id, :to_table_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("source_id", sourceId);
        UUIDHolder keyHolder = new UUIDHolder();
        for (DatasetMapTable table : tableList) {
            params.addValue("from_table_id", table.getFromTable().getId());
            params.addValue("to_table_id", table.getToTable().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            table.id(id);
            createColumns(id, table.getDatasetMapColumns());
        }
    }

    protected void createColumns(UUID tableId, Collection<DatasetMapColumn> columns) {
        String sql = "INSERT INTO dataset_map_column (map_table_id, from_column_id, to_column_id)" +
                " VALUES (:map_table_id, :from_column_id, :to_column_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("map_table_id", tableId);
        UUIDHolder keyHolder = new UUIDHolder();
        for (DatasetMapColumn column : columns) {
            params.addValue("from_column_id", column.getFromColumn().getId());
            params.addValue("type", column.getToColumn().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            column.id(id);
        }
    }

}
