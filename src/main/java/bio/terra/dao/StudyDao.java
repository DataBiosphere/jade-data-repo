package bio.terra.dao;

import bio.terra.metadata.Study;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class StudyDao extends MetaDao<Study> {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    @Autowired
    public StudyDao(NamedParameterJdbcTemplate jdbcTemplate,
                    TableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }


    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Study study) {
        String sql = "INSERT INTO study (name, description, created_date) VALUES (:name, :description, :createdDate)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", study.getName())
                .addValue("description", study.getDescription())
                .addValue("createdDate", new Timestamp(Instant.now().toEpochMilli()));
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = getIdKey(keyHolder);
        study.setId(studyId);
        tableDao.createStudyTables(study);
        relationshipDao.createStudyRelationships(study);
        assetDao.createAssets(study);
        return studyId;
    }

    private static final class StudyMapper implements RowMapper<Study> {

        public Study mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Study()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setName(rs.getString("name"))
                    .setDescription(rs.getString("description"))
                    .setCreatedDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class)));
        }
    }

        //    @Override
    public Study retrieve(UUID id) {
        Study study = jdbcTemplate.queryForObject(
                "SELECT id, name, description, created_date FROM study WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id),
                new StudyMapper());
        // needed for fix bugs. but really can't be null
        if (study != null) {
            tableDao.retrieve(study);
            relationshipDao.retrieve(study);
//            assetDao.retrieve(study);
        }
        return study;
    }
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
