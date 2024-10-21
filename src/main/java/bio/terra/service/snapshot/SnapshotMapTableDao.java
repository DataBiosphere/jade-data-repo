package bio.terra.service.snapshot;

import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Table;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotMapTableDao {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SnapshotMapTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  // part of a transaction propagated from SnapshotDao

  public void createTables(UUID sourceId, List<SnapshotMapTable> tableList) {
    String sql =
        "INSERT INTO snapshot_map_table (source_id, from_table_id, to_table_id)"
            + "VALUES (:source_id, :from_table_id, :to_table_id)";
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
    String sql =
        "INSERT INTO snapshot_map_column (map_table_id, from_column_id, to_column_id)"
            + " VALUES (:map_table_id, :from_column_id, :to_column_id)";
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
    String sql =
        """
SELECT smt.id map_table_id, from_table_id, to_table_id,
       smc.id map_column_id, from_column_id, to_column_id
FROM snapshot_map_table smt,
     snapshot_map_column smc
WHERE smt.source_id = :source_id
AND  smt.id = smc.map_table_id
    """;
    Map<UUID, SnapshotMapTable> mappingMap = new TreeMap<>();
    jdbcTemplate.query(
        sql,
        new MapSqlParameterSource().addValue("source_id", source.getId()),
        (rs, rowNum) -> {
          UUID mapTableId = rs.getObject("map_table_id", UUID.class);
          UUID fromTableId = rs.getObject("from_table_id", UUID.class);
          UUID toTableId = rs.getObject("to_table_id", UUID.class);
          UUID mapColumnId = rs.getObject("map_column_id", UUID.class);
          UUID fromColumnId = rs.getObject("from_column_id", UUID.class);
          UUID toColumnId = rs.getObject("to_column_id", UUID.class);

          DatasetTable datasetTable = (DatasetTable) getTable(fromTableId, source.getDataset());

          SnapshotTable snapshotTable = (SnapshotTable) getTable(toTableId, snapshot);

          SnapshotMapTable snapshotMapTable =
              mappingMap.computeIfAbsent(
                  mapTableId,
                  id ->
                      new SnapshotMapTable()
                          .id(id)
                          .fromTable(datasetTable)
                          .toTable(snapshotTable)
                          .snapshotMapColumns(new ArrayList<>()));

          snapshotMapTable
              .getSnapshotMapColumns()
              .add(
                  new SnapshotMapColumn()
                      .id(mapColumnId)
                      .fromColumn(getColumn(datasetTable, fromColumnId))
                      .toColumn(getColumn(snapshotTable, toColumnId)));

          return snapshotMapTable;
        });

    return List.copyOf(mappingMap.values());
  }

  private Table getTable(UUID tableId, FSContainerInterface container)
      throws CorruptMetadataException {
    var table =
        switch (container.getCollectionType()) {
          case DATASET -> ((Dataset) container).getTableById(tableId);
          case SNAPSHOT -> ((Snapshot) container).getTableById(tableId);
        };
    return table.orElseThrow(
        () ->
            new CorruptMetadataException(
                "Dataset table referenced by snapshot source map table was not found"));
  }

  private Column getColumn(Table table, UUID columnId) {
    return table
        .getColumnById(columnId)
        .orElseThrow(
            () ->
                new CorruptMetadataException(
                    "Dataset column referenced by snapshot source map column was not found"));
  }
}
