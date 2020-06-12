package bio.terra.service.resourcemanagement.google;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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

    /**
     * Insert a new row into the bucket_resource metadata table and give the provided flight the lock by setting the
     * flightid column. If there already exists a row with this bucket name, return null instead of throwing an
     * exception.
     * @param bucketRequest
     * @param flightId
     * @return a reference to the bucket as a POJO GoogleBucketResource if the insert succeeded, null otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public GoogleBucketResource createAndLockBucket(GoogleBucketRequest bucketRequest, String flightId) {
        GoogleProjectResource projectResource = Optional.ofNullable(bucketRequest.getGoogleProjectResource())
            .orElseThrow(IllegalArgumentException::new);

        // Put an end to serialization errors here. We only come through here if we really need to create
        // the bucket, so this is not on the path of most bucket lookups.
        jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE bucket_resource IN EXCLUSIVE MODE");

        String sql = "INSERT INTO bucket_resource (project_resource_id, name, flightid) VALUES " +
            "(:project_resource_id, :name, :flightid) " +
            "ON CONFLICT ON CONSTRAINT bucket_resource_name_key DO NOTHING";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("project_resource_id", projectResource.getRepositoryId())
            .addValue("name", bucketRequest.getBucketName())
            .addValue("flightid", flightId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        int numRowsUpdated = jdbcTemplate.update(sql, params, keyHolder);
        if (numRowsUpdated == 1) {
            return (new GoogleBucketResource(bucketRequest))
                .projectResource(projectResource)
                .name(bucketRequest.getBucketName())
                .resourceId(keyHolder.getId())
                .flightId(flightId);
        } else {
            return null;
        }
    }

    /**
     * Unlock an existing bucket_resource metadata row, by setting flightid = NULL.
     * Only the flight that currently holds the lock can unlock the row.
     * @param bucketName
     * @param flightId
     * @return true if a row was unlocked, false otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean unlockBucket(String bucketName, String flightId) {
        String sql = "UPDATE bucket_resource SET flightid = NULL " +
            "WHERE name = :name AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", bucketName)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        return (numRowsUpdated == 1);
    }

    /**
     * Fetch an existing bucket_resource metadata row using the name amd project id.
     * This method expects that there is exactly one row matching the provided name and project id.
     * @param bucketRequest
     * @return a reference to the bucket as a POJO GoogleBucketResource or null if not found
     * @throws GoogleResourceException if the bucket matches, but is in the wrong project
     * @throws CorruptMetadataException if multiple buckets have the same name
     */
    public GoogleBucketResource getBucket(GoogleBucketRequest bucketRequest) {
        String bucketName = bucketRequest.getBucketName();
        List<GoogleBucketResource> bucketResourcesByName =
            retrieveBucketsBy("name", bucketName, String.class);
        if (bucketResourcesByName == null || bucketResourcesByName.size() == 0) {
            return null;
        }
        if (bucketResourcesByName.size() > 1) {
            // this should never happen because name is unique in the PostGres table
            // this also never happen because Google bucket names are unique
            throw new CorruptMetadataException("Multiple buckets found with same name: " + bucketName);
        }

        GoogleBucketResource bucketResource = bucketResourcesByName.get(0);
        UUID foundProjectId = bucketResource.getProjectResource().getRepositoryId();
        UUID requestedProjectId = bucketRequest.getGoogleProjectResource().getRepositoryId();
        if (!foundProjectId.equals(requestedProjectId)) {
            // there is a bucket with this name in our metadata, but it's for a different project
            throw new GoogleResourceException(
                String.format("A bucket with this name already exists for a different project: %s, %s",
                    bucketName, requestedProjectId));
        }

        return bucketResource;
    }

    /**
     * Delete the bucket_resource metadata row associated with the bucket, provided the row is either unlocked or
     * locked by the provided flight.
     * @param bucketName
     * @param flightId
     * @return true if a row is deleted, false otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean deleteBucketMetadata(String bucketName, String flightId) {
        String sql = "DELETE FROM bucket_resource " +
            "WHERE name = :name AND (flightid = :flightid OR flightid IS NULL)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", bucketName)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        return (numRowsUpdated == 1);
    }

    private List<GoogleBucketResource> retrieveBucketsBy(String column, Object value, Class valueClass) {
        List<String> selects = Arrays.asList(
            // project_resource
            "p.id AS project_resource_id",
            "google_project_id",
            "google_project_number",
            "service_ids",
            "profile_id",

            // bucket_resource
            "b.id AS bucket_resource_id",
            "name",
            "flightid"
        );
        String query = "SELECT %s " +
            " FROM bucket_resource b JOIN project_resource p ON b.project_resource_id = p.id " +
            " WHERE b.%s = :%s";
        String sql = String.format(query, String.join(", ", selects), column, column);
        MapSqlParameterSource params = new MapSqlParameterSource().addValue(column, valueClass.cast(value));
        List<GoogleBucketResource> bucketResources = jdbcTemplate.query(sql, params, new DataBucketMapper());

        if (bucketResources.size() == 0) {
            return null;
        }
        return bucketResources;
    }

    /**
     * Fetch an existing bucket_resource metadata row using the id.
     * This method expects that there is exactly one row matching the provided resource id.
     * @param bucketResourceId
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws GoogleResourceNotFoundException if no bucket_resource metadata row is found
     */
    public GoogleBucketResource retrieveBucketById(UUID bucketResourceId) {
        List<GoogleBucketResource> bucketResources = retrieveBucketsBy("id", bucketResourceId, UUID.class);
        if (bucketResources == null) {
            throw new GoogleResourceNotFoundException("Bucket not found for id:" + bucketResourceId);
        }
        if (bucketResources.size() > 1) {
            throw new CorruptMetadataException(
                "Found more than one result for bucket resource id: " + bucketResourceId);
        }
        return bucketResources.get(0);
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
                .name(rs.getString("name"))
                .flightId(rs.getString("flightid"));
        }
    }
}
