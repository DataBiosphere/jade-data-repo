package bio.terra.dao;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.model.RelationshipTermModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class RelationshipDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public RelationshipDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from StudyDao
    public void createStudyRelationships(Study study) {
        for (StudyRelationship rel : study.getRelationships()) {
            create(rel);
        }
    }

    protected void create(StudyRelationship studyRelationship) {
        String sql = "INSERT INTO study_relationship " +
                "(name, from_cardinality, to_cardinality, from_table, from_column, to_table, to_column) VALUES " +
                "(:name, :from_cardinality, :to_cardinality, :from_table, :from_column, :to_table, :to_column)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", studyRelationship.getName())
                .addValue("from_cardinality", studyRelationship.getFromCardinality().toString())
                .addValue("to_cardinality", studyRelationship.getToCardinality().toString())
                .addValue("from_table", studyRelationship.getFrom().getInTable().getId())
                .addValue("from_column", studyRelationship.getFrom().getId())
                .addValue("to_table", studyRelationship.getTo().getInTable().getId())
                .addValue("to_column", studyRelationship.getTo().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID relationshipId = keyHolder.getId();
        studyRelationship.id(relationshipId);
    }

    public void retrieve(Study study) {
        List<UUID> columnIds = new ArrayList<>();
        study.getTables().forEach(table ->
                table.getColumns().forEach(column -> columnIds.add(column.getId())));
        study.relationships(retrieveStudyRelationships(columnIds, study.getAllColumnsById()));
    }

    private List<StudyRelationship> retrieveStudyRelationships(
            List<UUID> columnIds, Map<UUID,
            StudyTableColumn> columns) {
        String sql = "SELECT id, name, from_cardinality, to_cardinality, from_table, from_column, to_table, to_column "
                + "FROM study_relationship WHERE from_column IN (:columns) OR to_column IN (:columns)";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("columns", columnIds);
        return jdbcTemplate.query(sql, params, (rs, rowNum) ->
                new StudyRelationship()
                        .id(UUID.fromString(rs.getString("id")))
                        .name(rs.getString("name"))
                        .fromCardinality(RelationshipTermModel.CardinalityEnum.fromValue(
                                rs.getString("from_cardinality")))
                        .toCardinality(RelationshipTermModel.CardinalityEnum.fromValue(
                                rs.getString("to_cardinality")))
                        .from(columns.get(UUID.fromString(rs.getString("from_column"))))
                        .to(columns.get(UUID.fromString(rs.getString("to_column")))));
    }
}
