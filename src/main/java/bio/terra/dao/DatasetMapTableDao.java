package bio.terra.dao;

import bio.terra.dao.exceptions.CorruptMetadataException;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
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
        DaoKeyHolder keyHolder = new DaoKeyHolder();
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
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        for (DatasetMapColumn column : columns) {
            params.addValue("from_column_id", column.getFromColumn().getId());
            params.addValue("to_column_id", column.getToColumn().getId());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID id = keyHolder.getId();
            column.id(id);
        }
    }

    public List<DatasetMapTable> retrieveMapTables(Dataset dataset, DatasetSource source) {
        String sql = "SELECT id, source_id, from_table_id, to_table_id" +
                " FROM dataset_map_table WHERE source_id = :source_id";
        List<DatasetMapTable> mapTableList = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("source_id", source.getId()),
            (rs, rowNum) -> {
                List<DatasetMapTable> mapTables = new ArrayList<>();
                UUID fromTableId = UUID.fromString(rs.getString("from_table_id"));
                Optional<StudyTable> studyTable = source.getStudy().getTableById(fromTableId);
                if (!studyTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "Study table referenced by dataset source map table was not found!");
                }

                UUID toTableId = UUID.fromString(rs.getString("to_table_id"));
                Optional<Table> datasetTable = dataset.getTableById(toTableId);
                if (!datasetTable.isPresent()) {
                    throw new CorruptMetadataException(
                            "Dataset table referenced by dataset source map table was not found!");
                }

                UUID id = UUID.fromString(rs.getString("id"));
                List<DatasetMapColumn> mapColumns = retrieveMapColumns(id, studyTable.get(), datasetTable.get());

                return new DatasetMapTable()
                        .id(id)
                        .fromTable(studyTable.get())
                        .toTable(datasetTable.get())
                        .datasetMapColumns(mapColumns);
            });

        return mapTableList;
    }

    public List<DatasetMapColumn> retrieveMapColumns(UUID mapTableId, StudyTable fromTable, Table toTable) {
        String sql = "SELECT id, from_column_id, to_column_id" +
                " FROM dataset_map_column WHERE map_table_id = :map_table_id";

        List<DatasetMapColumn> mapColumns = jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("map_table_id", mapTableId),
            (rs, rowNum) -> {
                UUID fromId = UUID.fromString(rs.getString("from_column_id"));
                Optional<StudyTableColumn> studyColumn = fromTable.getColumnById(fromId);
                if (!studyColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "Study column referenced by dataset source map column was not found");
                }

                UUID toId = UUID.fromString(rs.getString("to_column_id"));
                Optional<Column> datasetColumn = toTable.getColumnById(toId);
                if (!datasetColumn.isPresent()) {
                    throw new CorruptMetadataException(
                            "Dataset column referenced by dataset source map column was not found");
                }

                return new DatasetMapColumn()
                        .id(UUID.fromString(rs.getString("from_column_id")))
                        .fromColumn(studyColumn.get())
                        .toColumn(datasetColumn.get());
            });

        return mapColumns;
    }

}
