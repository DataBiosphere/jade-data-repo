package bio.terra.datarepo.service.snapshot;

import bio.terra.datarepo.common.Column;
import bio.terra.datarepo.common.DaoKeyHolder;
import bio.terra.datarepo.common.Relationship;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Similar to DatasetRelationshipDao, might benefit from refactoring into an interface or abstract
 * class eventually
 */
@Repository
public class SnapshotRelationshipDao {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SnapshotRelationshipDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  // part of a transaction propagated from SnapshotDao
  public void createSnapshotRelationships(Snapshot snapshot) {
    for (Relationship rel : snapshot.getRelationships()) {
      create(rel);
    }
  }

  protected void create(Relationship relationship) {
    String sql =
        "INSERT INTO snapshot_relationship "
            + "(name, from_table, from_column, to_table, to_column) VALUES "
            + "(:name, :from_table, :from_column, :to_table, :to_column)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", relationship.getName())
            .addValue("from_table", relationship.getFromTable().getId())
            .addValue("from_column", relationship.getFromColumn().getId())
            .addValue("to_table", relationship.getToTable().getId())
            .addValue("to_column", relationship.getToColumn().getId());
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    jdbcTemplate.update(sql, params, keyHolder);
    UUID relationshipId = keyHolder.getId();
    relationship.id(relationshipId);
  }

  public void retrieve(Snapshot snapshot) {
    List<UUID> columnIds = new ArrayList<>();
    snapshot
        .getTables()
        .forEach(table -> table.getColumns().forEach(column -> columnIds.add(column.getId())));
    snapshot.relationships(
        retrieveSnapshotRelationships(
            columnIds, snapshot.getTablesById(), snapshot.getAllColumnsById()));
  }

  private List<Relationship> retrieveSnapshotRelationships(
      List<UUID> columnIds, Map<UUID, SnapshotTable> tables, Map<UUID, Column> columns) {
    String sql =
        "SELECT id, name, from_table, from_column, to_table, to_column "
            + "FROM snapshot_relationship WHERE from_column IN (:columns) OR to_column IN (:columns)";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("columns", columnIds);
    return jdbcTemplate.query(
        sql,
        params,
        (rs, rowNum) ->
            new Relationship()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"))
                .fromTable(tables.get(rs.getObject("from_table", UUID.class)))
                .fromColumn(columns.get(rs.getObject("from_column", UUID.class)))
                .toTable(tables.get(rs.getObject("to_table", UUID.class)))
                .toColumn(columns.get(rs.getObject("to_column", UUID.class))));
  }
}
