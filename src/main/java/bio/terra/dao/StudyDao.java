package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class StudyDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final StudyTableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    @Autowired
    public StudyDao(NamedParameterJdbcTemplate jdbcTemplate,
                    StudyTableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Study study) {
        String sql = "INSERT INTO study (name, description) VALUES (:name, :description)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", study.getName())
                .addValue("description", study.getDescription());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = keyHolder.getId();
        study.id(studyId);
        study.createdDate(keyHolder.getCreatedDate());
        tableDao.createTables(study.getId(), study.getTables());
        relationshipDao.createStudyRelationships(study);
        assetDao.createAssets(study);
        return studyId;
    }

    @Transactional
    public boolean delete(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM study WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    @Transactional
    public boolean deleteByName(String studyName) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM study WHERE name = :name",
                new MapSqlParameterSource().addValue("name", studyName));
        return rowsAffected > 0;
    }

    public Study retrieve(UUID id) {
        StudySummary summary = retrieveSummaryById(id);
        return retrieveWorker(summary);
    }

    public Study retrieveByName(String name) {
        StudySummary summary = retrieveSummaryByName(name);
        return retrieveWorker(summary);
    }

    private Study retrieveWorker(StudySummary summary) {
        Study study = null;
        try {
            if (summary != null) {
                study = new Study(summary);
                study.tables(tableDao.retrieveTables(study.getId()));
                relationshipDao.retrieve(study);
                assetDao.retrieve(study);
            }
            return study;
        } catch (EmptyResultDataAccessException ex) {
            throw new CorruptMetadataException("Inconsistent data", ex);
        }
    }

    public StudySummary retrieveSummaryById(UUID id) {
        try {
            String sql = "SELECT id, name, description, created_date FROM study WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new StudySummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StudyNotFoundException("Study not found for id " + id.toString());
        }
    }

    public StudySummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT id, name, description, created_date FROM study WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new StudySummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StudyNotFoundException("Study not found for name " + name);
        }
    }

    // does not return sub-objects with studies
    public List<StudySummary> enumerate(int offset, int limit) {
        String sql = "SELECT id, name, description, created_date FROM study " +
            "ORDER BY created_date OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("offset", offset)
            .addValue("limit", limit);
        return jdbcTemplate.query(sql, params, new StudySummaryMapper());
    }

    private static class StudySummaryMapper implements RowMapper<StudySummary> {
        public StudySummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudySummary()
                    .id(rs.getObject("id", UUID.class))
                    .name(rs.getString("name"))
                    .description(rs.getString("description"))
                    .createdDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class)));
        }
    }
}
