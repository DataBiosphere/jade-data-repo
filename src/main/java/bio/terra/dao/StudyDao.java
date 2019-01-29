package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Study;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class StudyDao extends MetaDao<Study> {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private TableDao tableDao;

    @Autowired
    private RelationshipDao relationshipDao;


    @Autowired
    public StudyDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }


    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Study study) {
        String sql = "INSERT INTO study (name, description, created_date) VALUES (:name, :description, :created_date)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", study.getName());
        params.addValue("description", study.getDescription());
        params.addValue("created_date", new Timestamp(Instant.now().toEpochMilli()));
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = getIdKey(keyHolder);
        study.setId(studyId);
        tableDao.createStudyTables(study);
        relationshipDao.createStudyRelationships(study);
        return studyId;
    }

//    @Override
//    public Study retrieve(String id) {
//        return null;
//    }
//
//    @Override
//    public void delete(String id) {
//
//    }
//
//    @Override
//    public List<Study> enumerate() {
//        return null;
//    }
}
