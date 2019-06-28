package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.metadata.Column;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotMapColumn;
import bio.terra.metadata.DataSnapshotMapTable;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.Table;
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
public class DataSnapshotMapTableDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DataSnapshotMapTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from DataSnapshotDao

    public void createTables(UUID sourceId, List<DataSnapshotMapTable> tableList) {
        String sql = "INSERT INTO datasnapshot_map_table (source_id, from_table_id, to_table_id)" +
                "VALUES (:source_id, :from_table_id, :to_table_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("source_id", sourceId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (DataSnapshotMapTable table : tableList) {
            params.addValue("from_table_id", table.getFromTable().getId());
            params.addValue("to_table_id", table.getToTable().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            table.id(id);
            createColumns(id, table.getDataSnapshotMapColumns());
        }
    }

    protected void createColumns(UUID tableId, Collection<DataSnapshotMapColumn> columns) {
        String sql = "INSERT INTO datasnapshot_map_column (map_table_id, from_column_id, to_column_id)" +
                " VALUES (:map_table_id, :from_column_id, :to_column_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("map_table_id", tableId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (DataSnapshotMapColumn column : columns) {
            params.addValue("from_column_id", column.getFromColumn().getId());
            params.addValue("to_column_id", column.getToColumn().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            column.id(id);
        }
    }

    public List<DataSnapshotMapTable> retrieveMapTables(DataSnapshot dataSnapshot, DataSnapshotSource source) {
        String sql = "SELECT id, source_id, from_table_id, to_table_id" +
                " FROM datasnapshot_map_table WHERE source_id = :source_id";
        List<DataSnapshotMapTable> mapTableList = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("source_id", source.getId()),
            (rs, rowNum) -> {
                List<DataSnapshotMapTable> mapTables = new ArrayList<>();
                UUID fromTableId = rs.getObject("from_table_id", UUID.class);
                Optional<Table> studyTable = source.getStudy().getTableById(fromTableId);
                if (!studyTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "Study table referenced by dataSnapshot source map table was not found!");
                }

                UUID toTableId = UUID.fromString(rs.getString("to_table_id"));
                Optional<Table> dataSnapshotTable = dataSnapshot.getTableById(toTableId);
                if (!dataSnapshotTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "DataSnapshot table referenced by dataSnapshot source map table was not found!");
                }

                UUID id = rs.getObject("id", UUID.class);
                List<DataSnapshotMapColumn> mapColumns = retrieveMapColumns(id, studyTable.get(), dataSnapshotTable.get());

                return new DataSnapshotMapTable()
                        .id(id)
                        .fromTable(studyTable.get())
                        .toTable(dataSnapshotTable.get())
                        .dataSnapshotMapColumns(mapColumns);
            });

        return mapTableList;
    }

    public List<DataSnapshotMapColumn> retrieveMapColumns(UUID mapTableId, Table fromTable, Table toTable) {
        String sql = "SELECT id, from_column_id, to_column_id" +
                " FROM datasnapshot_map_column WHERE map_table_id = :map_table_id";

        List<DataSnapshotMapColumn> mapColumns = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("map_table_id", mapTableId),
            (rs, rowNum) -> {
                UUID fromId = rs.getObject("from_column_id", UUID.class);
                Optional<Column> studyColumn = fromTable.getColumnById(fromId);
                if (!studyColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "Study column referenced by dataSnapshot source map column was not found");
                }

                UUID toId = rs.getObject("to_column_id", UUID.class);
                Optional<Column> dataSnapshotColumn = toTable.getColumnById(toId);
                if (!dataSnapshotColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "DataSnapshot column referenced by dataSnapshot source map column was not found");
                }

                return new DataSnapshotMapColumn()
                        .id(rs.getObject("from_column_id", UUID.class))
                        .fromColumn(studyColumn.get())
                        .toColumn(dataSnapshotColumn.get());
            });

        return mapColumns;
    }

}
