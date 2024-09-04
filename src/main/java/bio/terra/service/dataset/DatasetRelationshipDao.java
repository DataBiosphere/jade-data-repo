package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DatasetRelationshipDao {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public DatasetRelationshipDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * When creating new relationships, callers should note that relationships are assumed to exist
   * only between tables in the same dataset. The query powering relationship retrieval relies on
   * this assumption -- see {@link #retrieve(Dataset)}.
   *
   * @param relationships Dataset relationships to create in {@code dataset_relationship} table.
   *     Each relationship is assumed to only exist between tables in the same dataset.
   */
  public void createDatasetRelationships(List<Relationship> relationships) {
    for (Relationship rel : relationships) {
      create(rel);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  protected void create(Relationship relationship) {
    String sql =
        "INSERT INTO dataset_relationship "
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

  /**
   * Enrich the supplied dataset with its associated relationships.
   *
   * <p>Callers should note that relationships are assumed to exist only between tables in the same
   * dataset. The query powering this method relies on this assumption.
   *
   * @param dataset Dataset whose relationships will be populated from the {@code
   *     dataset_relationship} table. Each relationship is assumed to only exist between tables in
   *     the same dataset.
   */
  public void retrieve(Dataset dataset) {
    List<Relationship> relationships = retrieveDatasetRelationships(dataset);
    dataset.relationships(relationships);
  }

  private List<Relationship> retrieveDatasetRelationships(Dataset dataset) {
    String select =
        """
            SELECT r.id, r.name, r.from_table, r.from_column, r.to_table, r.to_column
            FROM dataset_relationship AS r
            -- Assumption: from_table and to_table will be in the same dataset, saving us a join.
            -- Relationships cannot exist between tables in different datasets.
            JOIN dataset_table AS dt ON (r.from_table = dt.id)
            WHERE dt.dataset_id = :dataset_id
            """;
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("dataset_id", dataset.getId());
    return jdbcTemplate.query(select, params, new RelationshipMapper(dataset));
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean delete(UUID id) {
    String sql = "DELETE FROM dataset_relationship WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    int rowsAffected = jdbcTemplate.update(sql, params);
    return rowsAffected > 0;
  }

  private static class RelationshipMapper implements RowMapper<Relationship> {
    private final Map<UUID, ? extends Table> tablesById;
    private final Map<UUID, Column> columnsById;

    /** A RowMapper to construct a Relationship enriched by a dataset's tables and columns. */
    public RelationshipMapper(Dataset dataset) {
      this.tablesById = dataset.getTablesById();
      this.columnsById = dataset.getAllColumnsById();
    }

    @Override
    public Relationship mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new Relationship()
          .id(rs.getObject("id", UUID.class))
          .name(rs.getString("name"))
          .fromTable(tablesById.get(rs.getObject("from_table", UUID.class)))
          .fromColumn(columnsById.get(rs.getObject("from_column", UUID.class)))
          .toTable(tablesById.get(rs.getObject("to_table", UUID.class)))
          .toColumn(columnsById.get(rs.getObject("to_column", UUID.class)));
    }
  }
}
