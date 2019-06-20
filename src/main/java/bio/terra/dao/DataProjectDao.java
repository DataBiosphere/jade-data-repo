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

    private StudyDataProjectSummary retrieveStudyDataProject(String sql, MapSqlParameterSource params) {
        try {
            return jdbcTemplate.queryForObject(sql, params, new StudyDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException("Study data project not found for: " + sql);
        }
    }

    public StudyDataProjectSummary retrieveStudyDataProjectById(UUID id) {
        String sql = "SELECT id, study_id, project_resource_id FROM study_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id);
        return retrieveStudyDataProject(sql, params);
    }

    public StudyDataProjectSummary retrieveStudyDataProjectByStudyId(UUID studyId) {
        String sql = "SELECT id, study_id, project_resource_id FROM study_data_project WHERE study_id = :study_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("study_id", studyId);
        return retrieveStudyDataProject(sql, params);
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

    private DatasetDataProjectSummary retrieveDatasetDataProject(String sql, MapSqlParameterSource params) {
        try {
            return jdbcTemplate.queryForObject(sql, params, new DatasetDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException("Dataset data project not found for: " + sql);
        }
    }

    public DatasetDataProjectSummary retrieveDatasetDataProjectById(UUID id) {
        String sql = "SELECT id, dataset_id, project_resource_id FROM dataset_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id);
        return retrieveDatasetDataProject(sql, params);
    }

    public DatasetDataProjectSummary retrieveDatasetDataProjectByDatasetId(UUID datasetId) {
        String sql = "SELECT id, dataset_id, project_resource_id FROM dataset_data_project WHERE dataset_id = :dataset_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", datasetId);
        return retrieveDatasetDataProject(sql, params);
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
