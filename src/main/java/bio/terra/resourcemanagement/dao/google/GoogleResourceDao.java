package bio.terra.resourcemanagement.dao.google;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.dao.DaoKeyHolder;
import bio.terra.dao.DaoUtils;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.resourcemanagement.service.exception.GoogleResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@Repository
public class GoogleResourceDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Connection connection;

    @Autowired
    public GoogleResourceDao(DataRepoJdbcConfiguration jdbcConfiguration) throws SQLException {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
        connection = jdbcConfiguration.getDataSource().getConnection();
    }

    public UUID createProject(GoogleProjectResource project) {
        try {
            String sql = "INSERT INTO project_resource " +
                "(google_project_id, google_project_number, profile_id, service_ids) VALUES " +
                "(:google_project_id, :google_project_number, :profile_id, :service_ids)";
            Array serviceIds = connection.createArrayOf("text", project.getServiceIds().toArray());
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("google_project_id", project.getGoogleProjectId())
                .addValue("google_project_number", project.getGoogleProjectNumber())
                .addValue("profile_id", project.getProfileId())
                .addValue("service_ids", serviceIds);
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            return keyHolder.getId();
        } catch (SQLException e) {
            throw new GoogleResourceException("Can't save project resource: " + project.getGoogleProjectId());
        }
    }

    private GoogleProjectResource retrieveProjectBy(String column, UUID value) {
        try {
            String sql = String.format("SELECT * FROM project_resource WHERE %s = :%s LIMIT 1", column, column);
            MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, value);
            return jdbcTemplate.queryForObject(sql, params, new DataProjectMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new GoogleResourceNotFoundException(String.format("Project not found for %s: %s", column, value));
        }
    }

    public GoogleProjectResource retrieveProjectById(UUID id) {
        return retrieveProjectBy("id", id);
    }

    public GoogleProjectResource retrieveProjectByGoogleProjectId(UUID googleProjectId) {
        return retrieveProjectBy("google_project_id", googleProjectId);
    }

    public boolean deleteProject(UUID id) {
        String sql = "DELETE FROM project_resource WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class DataProjectMapper implements RowMapper<GoogleProjectResource> {
        public GoogleProjectResource mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new GoogleProjectResource()
                .googleProjectId(rs.getString("google_project_id"))
                .googleProjectNumber(rs.getString("google_project_number"))
                .repositoryId(rs.getObject("id", UUID.class))
                .profileId(rs.getObject("profile_id", UUID.class))
                .serviceIds(DaoUtils.getStringList(rs, "service_ids"));
        }
    }
}
