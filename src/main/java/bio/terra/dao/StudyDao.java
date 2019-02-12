package bio.terra.dao;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
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
public class StudyDao extends StudySummaryDao {

    private final TableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    @Autowired
    public StudyDao(NamedParameterJdbcTemplate jdbcTemplate,
                    TableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao) {
        super(jdbcTemplate);
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }


    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Study study) {
        UUID studyId = super.create(study);
        tableDao.createStudyTables(study);
        relationshipDao.createStudyRelationships(study);
        assetDao.createAssets(study);
        return studyId;
    }

    @Transactional
    public boolean delete(UUID id) {
        int rowsAffected = getJdbcTemplate().update("DELETE FROM study WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    @Transactional
    public boolean deleteByName(String studyName) {
        int rowsAffected = getJdbcTemplate().update("DELETE FROM study WHERE name = :name",
                new MapSqlParameterSource().addValue("name", studyName));
        return rowsAffected > 0;
    }

    public Optional<Study> retrieve(UUID id) {
        Study study = null;
        try {
            StudySummary summary = super.retrieve(id);
            if (summary != null) {
                study = new Study(summary);
                tableDao.retrieve(study);
                relationshipDao.retrieve(study);
                assetDao.retrieve(study);
            }
            return Optional.of(study);
        } catch (
                EmptyResultDataAccessException ex) {
            return Optional.empty();
        }
    }
}
