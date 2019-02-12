package bio.terra.dao;

import bio.terra.metadata.Study;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public class StudyDao {

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
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", study.getName())
                .addValue("description", study.getDescription())
                .addValue("createdDate", new Timestamp(now.toEpochMilli()));
        UUIDHolder keyHolder = new UUIDHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = keyHolder.getId();
        study.setId(studyId);
        study.setCreatedDate(now);
        tableDao.createStudyTables(study);
        relationshipDao.createStudyRelationships(study);
        assetDao.createAssets(study);
        return studyId;
    }

    @Transactional
    public void delete(UUID id) {
        jdbcTemplate.update("DELETE FROM study WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));

    }

    public Study retrieve(UUID id) {
        Study study = jdbcTemplate.queryForObject(
                "SELECT id, name, description, created_date FROM study WHERE id = :id",
                //this is a hack for check style. if the lambda params are on the next line it fails indentation check
                new MapSqlParameterSource().addValue("id", id), (
                        rs, rowNum) -> new Study()
                        .setId(UUID.fromString(rs.getString("id")))
                        .setName(rs.getString("name"))
                        .setDescription(rs.getString("description"))
                        .setCreatedDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class))));
        // needed for fix bugs. but really can't be null
        if (study != null) {
            tableDao.retrieve(study);
            relationshipDao.retrieve(study);
            assetDao.retrieve(study);
        }
        return study;
    }

    public Optional<Study> retrieveByName(String studyName) {
        String sql = "SELECT id, name, description, created_date FROM study WHERE name = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", studyName);
        try {
            Study study = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    new Study()
                            .setId(UUID.fromString(rs.getString("id")))
                            .setName(rs.getString("name"))
                            .setDescription(rs.getString("description"))
                            .setCreatedDate(Instant.from(rs.getObject("created_date",
                                    OffsetDateTime.class))));
            // needed for fix bugs. but really can't be null
            if (study != null) {
                tableDao.retrieve(study);
                relationshipDao.retrieve(study);
                assetDao.retrieve(study);
            }
            return Optional.of(study);
        } catch (EmptyResultDataAccessException ex) {
            return Optional.empty();
        }
    }
}
