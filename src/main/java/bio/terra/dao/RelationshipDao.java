package bio.terra.dao;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.model.RelationshipTermModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class RelationshipDao extends MetaDao<StudyRelationship> {

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
                "(name, from_cardinality, to_cardinality, from_column, to_column) VALUES " +
                "(:name, :from_cardinality, :to_cardinality, :from_column, :to_column)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", studyRelationship.getName());
        params.addValue("from_cardinality", studyRelationship.getFromCardinality().toString());
        params.addValue("to_cardinality", studyRelationship.getToCardinality().toString());
        params.addValue("from_column", studyRelationship.getFrom().getId());
        params.addValue("to_column", studyRelationship.getTo().getId());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID relationshipId = getIdKey(keyHolder);
        studyRelationship.setId(relationshipId);
    }

//    private static final class StudyRelationshipMapper implements RowMapper<StudyRelationship> {
//        public StudyRelationship mapRow(ResultSet rs, int rowNum) throws SQLException {
//            return new StudyRelationship()
//                    .setId(UUID.fromString(rs.getString("id")))
//                    .setName(rs.getString("name"))
//                    .setFromCardinality(RelationshipTermModel.CardinalityEnum.valueOf(
// rs.getString("from_cardinality")))
//                    .setToCardinality(RelationshipTermModel.CardinalityEnum.valueOf(rs.getString("to_cardinality")))
//                    .setFromColumn(rs.getString("from_cardinality")))
//        }
//    }

    //    @Override
    public void retrieve(Study study) {
        List<UUID> columnIds = new ArrayList<>();
        study.getTables().forEach(table ->
                table.getColumns().forEach(column -> columnIds.add(column.getId())));
        List<Map<String, Object>> results = retrieveStudyRelationships(columnIds);
        List<StudyRelationship> relationships = createRelationships(study, results);
        study.setRelationships(relationships);
    }

    private List<Map<String, Object>> retrieveStudyRelationships(List<UUID> columnIds) {
        List<Map<String, Object>> reldata = jdbcTemplate.queryForList(
                "SELECT id, name, from_cardinality, to_cardinality, from_column, to_column " +
                        "FROM study_relationship WHERE from_column IN (:columns) OR to_column IN (:columns)",
                new MapSqlParameterSource().addValue("columns", columnIds)
                );
        return reldata;
    }

    private List<StudyRelationship> createRelationships(Study study, List<Map<String, Object>> results) {
        // build up the collection of all columns
        Map<UUID, StudyTableColumn> columns = new HashMap<>();
        study.getTables().forEach(table -> table.getColumns().forEach(column -> columns.put(column.getId(), column)));

        return results
                .stream()
                .map(rs ->
                        new StudyRelationship()
                                .setId(UUID.fromString(rs.get("id").toString()))
                                .setName(rs.get("name").toString())
                                .setFromCardinality(RelationshipTermModel.CardinalityEnum.fromValue(rs.get(
                                        "from_cardinality").toString()))
                                .setToCardinality(RelationshipTermModel.CardinalityEnum.fromValue(rs.get(
                                        "to_cardinality").toString()))
                                .setFrom(columns.get(UUID.fromString(rs.get("from_column").toString())))
                                .setTo(columns.get(UUID.fromString(rs.get("to_column").toString()))))
                .collect(Collectors.toList());
    }
}
