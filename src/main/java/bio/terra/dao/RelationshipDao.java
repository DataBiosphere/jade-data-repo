package bio.terra.dao;

import bio.terra.metadata.Relationship;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.model.RelationshipModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class RelationshipDao extends MetaDao<Relationship> {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public RelationshipDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    void createStudyRelationships(Study study) {
        for (Relationship rel : study.getRelationships().values()) {
            create(rel, study.getTables());
        }
    }

    public void create(Relationship relationship, Map<String, StudyTable> tables) {
        String sql = "INSERT INTO study_relationship (name, from_cardinality, to_cardinality, from_column, to_column) " +
                "VALUES (:name, :from_cardinality, :to_cardinality, :from_column, :to_column)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", relationship.getName());
        StudyTableColumn from = relationship.getFrom();
        StudyTableColumn to = relationship.getTo();
        params.addValue("from_cardinality", relationship.getFromCardinality().toString());
        params.addValue("to_cardinality", relationship.getToCardinality().toString());
        params.addValue("from_column", relationship.getFrom().getId());
        params.addValue("to_column", relationship.getTo().getId());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID relationshipId = getIdKey(keyHolder);
        relationship.setId(relationshipId);
    }

    }
