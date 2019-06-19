package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.dao.exception.DataProjectNotFoundException;
import bio.terra.metadata.DatasetDataProjectSummary;
import bio.terra.metadata.StudyDataProjectSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@Repository
public class DataProjectDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DataProjectDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    public UUID createStudyDataProject(StudyDataProjectSummary studyDataProjectSummary) {
        String sql = "INSERT INTO study_data_project (study_id, project_resource_id) VALUES " +
            "(:study_id, :project_resource_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("study_id", studyDataProjectSummary.getStudyId())
            .addValue("project_resource_id", studyDataProjectSummary.getProjectResourceId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    private StudyDataProjectSummary retrieveStudyDataProjectByColumn(String column, UUID value) {
        try {
            String sql = String.format("SELECT * FROM study_data_project WHERE %s = :%s", column, column);
            MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, value);
            return jdbcTemplate.queryForObject(sql, params, new StudyDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException(
                String.format("Study data project not found for '%s': %s", column, value));
        }
    }

    public StudyDataProjectSummary retrieveStudyDataProjectById(UUID id) {
        return retrieveStudyDataProjectByColumn("id", id);
    }

    public StudyDataProjectSummary retrieveStudyDataProjectByStudyId(UUID studyId) {
        return retrieveStudyDataProjectByColumn("study_id", studyId);
    }

    public boolean deleteStudyDataProject(UUID id) {
        String sql = "DELETE FROM study_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class StudyDataProjectSummaryMapper implements RowMapper<StudyDataProjectSummary> {
        public StudyDataProjectSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudyDataProjectSummary()
                .id(rs.getObject("id", UUID.class))
                .studyId(rs.getObject("study_id", UUID.class))
                .projectResourceId(rs.getObject("project_resource_id", UUID.class));
        }
    }

    public UUID createDatasetDataProject(DatasetDataProjectSummary datasetDataProjectSummary) {
        String sql = "INSERT INTO dataset_data_project (dataset_id, project_resource_id) VALUES " +
            "(:dataset_id, :project_resource_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", datasetDataProjectSummary.getDatasetId())
            .addValue("project_resource_id", datasetDataProjectSummary.getProjectResourceId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    private DatasetDataProjectSummary retrieveDatasetDataProjectByColumn(String column, UUID value) {
        try {
            String sql = String.format("SELECT * FROM dataset_project_resource WHERE %s = :%s", column, column);
            MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, value);
            return jdbcTemplate.queryForObject(sql, params, new DatasetDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException(
                String.format("Dataset data project not found for '%s': %s", column, value));
        }
    }

    public DatasetDataProjectSummary retrieveDatasetDataProjectById(UUID id) {
        return retrieveDatasetDataProjectByColumn("id", id);
    }

    public DatasetDataProjectSummary retrieveDatasetDataProjectByDatasetId(UUID datasetId) {
        return retrieveDatasetDataProjectByColumn("dataset_id", datasetId);
    }

    public boolean deleteDatasetDataProject(UUID id) {
        String sql = "DELETE FROM dataset_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class DatasetDataProjectSummaryMapper implements RowMapper<DatasetDataProjectSummary> {
        public DatasetDataProjectSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DatasetDataProjectSummary()
                .id(rs.getObject("id", UUID.class))
                .datasetId(rs.getObject("dataset_id", UUID.class))
                .projectResourceId(rs.getObject("project_resource_id", UUID.class));
        }
    }
}
