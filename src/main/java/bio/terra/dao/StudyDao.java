package bio.terra.dao;

import bio.terra.dao.exception.RepositoryMetadataException;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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

    public Study retrieve(UUID id) {
        StudySummary summary = super.retrieve(id);
        return retrieveWorker(summary);
    }

    public Study retrieveByName(String name) {
        StudySummary summary = super.retrieveByName(name);
        return retrieveWorker(summary);
    }

    private Study retrieveWorker(StudySummary summary) {
        Study study = null;
        try {
            if (summary != null) {
                study = new Study(summary);
                tableDao.retrieve(study);
                relationshipDao.retrieve(study);
                assetDao.retrieve(study);
            }
            return study;
        } catch (EmptyResultDataAccessException ex) {
            throw new RepositoryMetadataException("Inconsistent data", ex);
        }
    }

}
