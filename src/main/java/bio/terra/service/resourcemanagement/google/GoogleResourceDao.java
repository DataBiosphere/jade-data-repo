package bio.terra.service.resourcemanagement.google;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("google_project_id", project.getGoogleProjectId())
                .addValue("google_project_number", project.getGoogleProjectNumber())
                .addValue("profile_id", project.getProfileId())
                .addValue("service_ids", DaoUtils.createSqlStringArray(connection, project.getServiceIds()));
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            return keyHolder.getId();
        } catch (SQLException e) {
            throw new GoogleResourceException("Can't save project resource: " + project.getGoogleProjectId(), e);
        }
    }

    private GoogleProjectResource retrieveProjectBy(String column, Object value) {
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

    public GoogleProjectResource retrieveProjectByGoogleProjectId(String googleProjectId) {
        return retrieveProjectBy("google_project_id", googleProjectId);
    }

    public GoogleProjectResource retrieveProjectByProfileId(UUID profileId) {
        return retrieveProjectBy("profile_id", profileId);
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

    public UUID createBucket(GoogleBucketResource bucketResource) {
        GoogleProjectResource projectResource = Optional.ofNullable(bucketResource.getProjectResource())
            .orElseThrow(IllegalArgumentException::new);
        String sql = "INSERT INTO bucket_resource (project_resource_id, name) VALUES " +
            "(:project_resource_id, :name)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("project_resource_id", projectResource.getRepositoryId())
            .addValue("name", bucketResource.getName());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    private List<GoogleBucketResource> retrieveBucketsBy(String column, UUID value) {
        List<String> selects = Arrays.asList(
            // project_resource
            "p.id AS project_resource_id",
            "google_project_id",
            "google_project_number",
            "service_ids",
            "profile_id",

            // bucket_resource
            "b.id AS bucket_resource_id",
            "name"
        );
        String query = "SELECT %s " +
            " FROM bucket_resource b JOIN project_resource p ON b.project_resource_id = p.id " +
            " WHERE b.%s = :%s";
        String sql = String.format(query, String.join(", ", selects), column, column);
        MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, value);
        List<GoogleBucketResource> bucketResources = jdbcTemplate.query(sql, params, new DataBucketMapper());

        if (bucketResources.size() == 0) {
            throw new GoogleResourceNotFoundException(String.format("Bucket not found for %s: %s", column, value));
        }
        return bucketResources;
    }

    public GoogleBucketResource retrieveBucketById(UUID bucketResourceId) {
        List<GoogleBucketResource> bucketResources = retrieveBucketsBy("id", bucketResourceId);
        if (bucketResources.size() > 1) {
            throw new IllegalStateException(
                String.format("Found more than one result for bucket resource id: %s", bucketResourceId));
        }
        return bucketResources.get(0);
    }

    public List<GoogleBucketResource> retrieveBucketsByProjectResource(GoogleProjectResource projectResource) {
        return retrieveBucketsBy("project_resource_id", projectResource.getRepositoryId());
    }

    private static class DataBucketMapper implements RowMapper<GoogleBucketResource> {
        public GoogleBucketResource mapRow(ResultSet rs, int rowNum) throws SQLException {
            // create project resource, use that to construct bucket resource
            GoogleProjectResource projectResource = new GoogleProjectResource()
                .repositoryId(rs.getObject("project_resource_id", UUID.class))
                .googleProjectId(rs.getString("google_project_id"))
                .googleProjectNumber(rs.getString("google_project_number"))
                .serviceIds(DaoUtils.getStringList(rs, "service_ids"))
                .profileId(rs.getObject("profile_id", UUID.class));
            return new GoogleBucketResource()
                .projectResource(projectResource)
                .resourceId(rs.getObject("bucket_resource_id", UUID.class))
                .name(rs.getString("name"));
        }
    }
}
