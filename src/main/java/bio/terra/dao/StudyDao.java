package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.InvalidStudyException;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
import bio.terra.resourcemanagement.dao.ProfileDao;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class StudyDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final StudyTableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;
    private final ProfileDao profileDao;
    private final Connection connection;

    @Autowired
    public StudyDao(DataRepoJdbcConfiguration jdbcConfiguration,
                    StudyTableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao,
                    ProfileDao profileDao) throws SQLException {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
        connection = jdbcConfiguration.getDataSource().getConnection();
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
        this.profileDao = profileDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Study study) {
        try {
            String sql = "INSERT INTO study (name, description, default_profile_id, additional_profile_ids) VALUES " +
                "(:name, :description, :default_profile_id, :additional_profile_ids)";
            Array additionalProfileIds = DaoUtils.createSqlUUIDArray(connection, study.getAdditionalProfileIds());
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", study.getName())
                .addValue("description", study.getDescription())
                .addValue("default_profile_id", study.getDefaultProfileId())
                .addValue("additional_profile_ids", additionalProfileIds);
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            UUID studyId = keyHolder.getId();
            study.id(studyId);
            study.createdDate(keyHolder.getCreatedDate());
            tableDao.createTables(study.getId(), study.getTables());
            relationshipDao.createStudyRelationships(study);
            assetDao.createAssets(study);
            return studyId;
        } catch (DuplicateKeyException | SQLException e) {
            throw new InvalidStudyException("Cannot create study: " + study.getName(), e);
        }
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
                study.defaultProfile(profileDao.getBillingProfileById(summary.getDefaultProfileId()));
                List<UUID> additionalProfileIds = summary.getAdditionalProfileIds();
                if (additionalProfileIds != null) {
                    study.additionalProfiles(additionalProfileIds
                        .stream()
                        .map(profileDao::getBillingProfileById)
                        .collect(Collectors.toList()));
                }
            }
            return study;
        } catch (EmptyResultDataAccessException ex) {
            throw new CorruptMetadataException("Inconsistent data", ex);
        }
    }

    public StudySummary retrieveSummaryById(UUID id) {
        try {
            String sql = "SELECT * FROM study WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new StudySummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StudyNotFoundException("Study not found for id " + id.toString());
        }
    }

    public StudySummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT * FROM study WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new StudySummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StudyNotFoundException("Study not found for name " + name);
        }
    }

    // does not return sub-objects with studies
    public MetadataEnumeration<StudySummary> enumerate(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> accessibleStudyIds
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        DaoUtils.addAuthzIdsClause(accessibleStudyIds, params, whereClauses);

        // get total count of objects
        String countSql = "SELECT count(id) AS total FROM study WHERE " +
            StringUtils.join(whereClauses, " AND ");
        Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);

        // add the filter to the clause to get the actual items
        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }
        String sql = "SELECT id, name, description, created_date, default_profile_id FROM study " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<StudySummary> summaries = jdbcTemplate.query(sql, params, new StudySummaryMapper());

        return new MetadataEnumeration<StudySummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    private static class StudySummaryMapper implements RowMapper<StudySummary> {
        public StudySummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudySummary()
                    .id(rs.getObject("id", UUID.class))
                    .name(rs.getString("name"))
                    .description(rs.getString("description"))
                    .defaultProfileId(rs.getObject("default_profile_id", UUID.class))
                    .additionalProfileIds(DaoUtils.getUUIDList(rs, "additional_profile_ids"))
                    .createdDate(rs.getTimestamp("created_date").toInstant());
        }
    }
}
