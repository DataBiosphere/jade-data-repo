package bio.terra.dao.google;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.dao.DaoKeyHolder;
import bio.terra.dao.exception.google.ProjectNotFoundException;
import bio.terra.metadata.google.GoogleProject;
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
public class GoogleResourceDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public GoogleResourceDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    public UUID createProject(GoogleProject project) {
        String sql = "INSERT INTO project_resource (google_project_id, study_id, dataset_id, profile_id) VALUES " +
            "(:google_project_id, :study_id, :dataset_id, :profile_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("google_project_id", project.getGoogleProjectId())
            .addValue("study_id", project.getStudyId())
            .addValue("dataset_id", project.getDatasetId())
            .addValue("profile_id", project.getProfileId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    public GoogleProject retrieveProjectBy(String column, UUID value) {
        try {
            String sql = String.format("SELECT * FROM project_resource WHERE %s = :%s LIMIT 1", column, column);
            MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, value);
            return jdbcTemplate.queryForObject(sql, params, new GoogleProjectMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new ProjectNotFoundException(String.format("Project not found for %s: %s", column, value));
        }
    }

    public boolean deleteProject(UUID id) {
        String sql = "DELETE FROM project_resource WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class GoogleProjectMapper implements RowMapper<GoogleProject> {
        public GoogleProject mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new GoogleProject()
                .repositoryId(rs.getObject("id", UUID.class))
                .profileId(rs.getObject("profile_id", UUID.class))
                .studyId(rs.getObject("study_id", UUID.class))
                .datasetId(rs.getObject("dataset_id", UUID.class))
                .googleProjectId(rs.getString("google_project_id"));
        }
    }
}
