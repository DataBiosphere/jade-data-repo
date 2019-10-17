package bio.terra.service.snapshot;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Column;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.common.Table;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SnapshotMapTableDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public SnapshotMapTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from SnapshotDao

    public void createTables(UUID sourceId, List<SnapshotMapTable> tableList) {
        String sql = "INSERT INTO snapshot_map_table (source_id, from_table_id, to_table_id)" +
                "VALUES (:source_id, :from_table_id, :to_table_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("source_id", sourceId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (SnapshotMapTable table : tableList) {
            params.addValue("from_table_id", table.getFromTable().getId());
            params.addValue("to_table_id", table.getToTable().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            table.id(id);
            createColumns(id, table.getSnapshotMapColumns());
        }
    }

    protected void createColumns(UUID tableId, Collection<SnapshotMapColumn> columns) {
        String sql = "INSERT INTO snapshot_map_column (map_table_id, from_column_id, to_column_id)" +
                " VALUES (:map_table_id, :from_column_id, :to_column_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("map_table_id", tableId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (SnapshotMapColumn column : columns) {
            params.addValue("from_column_id", column.getFromColumn().getId());
            params.addValue("to_column_id", column.getToColumn().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            column.id(id);
        }
    }

    public List<SnapshotMapTable> retrieveMapTables(Snapshot snapshot, SnapshotSource source) {
        String sql = "SELECT id, source_id, from_table_id, to_table_id" +
                " FROM snapshot_map_table WHERE source_id = :source_id";
        List<SnapshotMapTable> mapTableList = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("source_id", source.getId()),
            (rs, rowNum) -> {
                List<SnapshotMapTable> mapTables = new ArrayList<>();
                UUID fromTableId = rs.getObject("from_table_id", UUID.class);
                Optional<DatasetTable> datasetTable = source.getDataset().getTableById(fromTableId);
                if (!datasetTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "Dataset table referenced by snapshot source map table was not found!");
                }

                UUID toTableId = UUID.fromString(rs.getString("to_table_id"));
                Optional<Table> snapshotTable = snapshot.getTableById(toTableId);
                if (!snapshotTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "Snapshot table referenced by snapshot source map table was not found!");
                }

                UUID id = rs.getObject("id", UUID.class);
                List<SnapshotMapColumn> mapColumns = retrieveMapColumns(id, datasetTable.get(), snapshotTable.get());

                return new SnapshotMapTable()
                        .id(id)
                        .fromTable(datasetTable.get())
                        .toTable(snapshotTable.get())
                        .snapshotMapColumns(mapColumns);
            });

        return mapTableList;
    }

    public List<SnapshotMapColumn> retrieveMapColumns(UUID mapTableId, Table fromTable, Table toTable) {
        String sql = "SELECT id, from_column_id, to_column_id" +
                " FROM snapshot_map_column WHERE map_table_id = :map_table_id";

        List<SnapshotMapColumn> mapColumns = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("map_table_id", mapTableId),
            (rs, rowNum) -> {
                UUID fromId = rs.getObject("from_column_id", UUID.class);
                Optional<Column> datasetColumn = fromTable.getColumnById(fromId);
                if (!datasetColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "Dataset column referenced by snapshot source map column was not found");
                }

                UUID toId = rs.getObject("to_column_id", UUID.class);
                Optional<Column> snapshotColumn = toTable.getColumnById(toId);
                if (!snapshotColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "Snapshot column referenced by snapshot source map column was not found");
                }

                return new SnapshotMapColumn()
                        .id(rs.getObject("from_column_id", UUID.class))
                        .fromColumn(datasetColumn.get())
                        .toColumn(snapshotColumn.get());
            });

        return mapColumns;
    }

}
