package bio.terra.service.resourcemanagement.google;

import bio.terra.common.DaoKeyHolder;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Repository
public class GoogleResourceDao {
    private static final Logger logger = LoggerFactory.getLogger(GoogleResourceDao.class);
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String defaultRegion;

    private static final String sqlProjectRetrieve = "SELECT id, google_project_id, google_project_number, profile_id" +
        " FROM project_resource";
    private static final String sqlProjectRetrieveById = sqlProjectRetrieve + " WHERE id = :id";
    private static final String sqlProjectRetrieveByProjectId = sqlProjectRetrieve +
        " WHERE google_project_id = :google_project_id";

    private static final String sqlBucketRetrieve =
        "SELECT p.id AS project_resource_id, google_project_id, google_project_number, profile_id," +
            " b.id AS bucket_resource_id, name, region, flightid" +
            " FROM bucket_resource b JOIN project_resource p ON b.project_resource_id = p.id ";
    private static final String sqlBucketRetrievedById = sqlBucketRetrieve + " WHERE b.id = :id";
    private static final String sqlBucketRetrievedByName = sqlBucketRetrieve + " WHERE b.name = :name";

    @Autowired
    public GoogleResourceDao(NamedParameterJdbcTemplate jdbcTemplate,
                             GcsConfiguration gcsConfiguration) throws SQLException {
        this.jdbcTemplate = jdbcTemplate;
        this.defaultRegion = gcsConfiguration.getRegion();
    }

    // -- project resource methods --

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public UUID createProject(GoogleProjectResource project) {
        String sql = "INSERT INTO project_resource " +
            "(google_project_id, google_project_number, profile_id) VALUES " +
            "(:google_project_id, :google_project_number, :profile_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("google_project_id", project.getGoogleProjectId())
            .addValue("google_project_number", project.getGoogleProjectNumber())
            .addValue("profile_id", project.getProfileId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public GoogleProjectResource retrieveProjectById(UUID id) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        return retrieveProjectBy(sqlProjectRetrieveById, params);
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public GoogleProjectResource retrieveProjectByGoogleProjectId(String googleProjectId) {
        MapSqlParameterSource params =
            new MapSqlParameterSource().addValue("google_project_id", googleProjectId);
        return retrieveProjectBy(sqlProjectRetrieveByProjectId, params);
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void deleteProject(UUID id) {
        String sql = "DELETE FROM project_resource WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        logger.info("Project resource {} was {}", id, (rowsAffected > 0 ? "deleted" : "not found"));
    }

    private GoogleProjectResource retrieveProjectBy(String sql, MapSqlParameterSource params) {
        try {
            return jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                new GoogleProjectResource()
                    .googleProjectId(rs.getString("google_project_id"))
                    .googleProjectNumber(rs.getString("google_project_number"))
                    .id(rs.getObject("id", UUID.class))
                    .profileId(rs.getObject("profile_id", UUID.class)));
        } catch (EmptyResultDataAccessException ex) {
            throw new GoogleResourceNotFoundException("Project not found");
        }
    }

    // -- bucket resource methods --

    /**
     * Insert a new row into the bucket_resource metadata table and give the provided flight the lock by setting the
     * flightid column. If there already exists a row with this bucket name, return null instead of throwing an
     * exception.
     * c     * @return a GoogleBucketResource if the insert succeeded, null otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public GoogleBucketResource createAndLockBucket(String bucketName,
                                                    GoogleProjectResource projectResource,
                                                    String flightId) {
        // Put an end to serialization errors here. We only come through here if we really need to create
        // the bucket, so this is not on the path of most bucket lookups.
        jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE bucket_resource IN EXCLUSIVE MODE");

        String sql = "INSERT INTO bucket_resource (project_resource_id, name, flightid) VALUES " +
            "(:project_resource_id, :name, :flightid) " +
            "ON CONFLICT ON CONSTRAINT bucket_resource_name_key DO NOTHING";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("project_resource_id", projectResource.getId())
            .addValue("name", bucketName)
            .addValue("flightid", flightId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();

        int numRowsUpdated = jdbcTemplate.update(sql, params, keyHolder);
        if (numRowsUpdated == 1) {
            return new GoogleBucketResource()
                .resourceId(keyHolder.getId())
                .flightId(flightId)
                .profileId(projectResource.getProfileId())
                .projectResource(projectResource)
                .name(bucketName);
        } else {
            return null;
        }
    }

    /**
     * Unlock an existing bucket_resource metadata row, by setting flightid = NULL.
     * Only the flight that currently holds the lock can unlock the row.
     * The lock may not be held - that is not an error
     *
     * @param bucketName bucket to unlock
     * @param flightId   flight trying to unlock it
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void unlockBucket(String bucketName, String flightId) {
        String sql = "UPDATE bucket_resource SET flightid = NULL " +
            "WHERE name = :name AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", bucketName)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        logger.info("Bucket {} was {}", bucketName, (numRowsUpdated > 0 ? "unlocked" : "not locked"));
    }

    /**
     * Fetch an existing bucket_resource metadata row using the name amd project id.
     * This method expects that there is exactly one row matching the provided name and project id.
     *
     * @param bucketName         name of the bucket
     * @param requestedProjectId projectId in which we are searching for the bucket
     * @return a reference to the bucket as a POJO GoogleBucketResource or null if not found
     * @throws GoogleResourceException  if the bucket matches, but is in the wrong project
     * @throws CorruptMetadataException if multiple buckets have the same name
     */
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public GoogleBucketResource getBucket(String bucketName, UUID requestedProjectId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", bucketName);
        GoogleBucketResource bucketResource = retrieveBucketBy(sqlBucketRetrievedByName, params);
        if (bucketResource == null) {
            return null;
        }

        UUID foundProjectId = bucketResource.getProjectResource().getId();
        if (!foundProjectId.equals(requestedProjectId)) {
            // there is a bucket with this name in our metadata, but it's for a different project
            throw new GoogleResourceException(
                String.format("A bucket with this name already exists for a different project: %s, %s",
                    bucketName, requestedProjectId));
        }

        return bucketResource;
    }

    /**
     * Fetch an existing bucket_resource metadata row using the id.
     * This method expects that there is exactly one row matching the provided resource id.
     *
     * @param bucketResourceId unique idea of a bucket resource
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws GoogleResourceNotFoundException if no bucket_resource metadata row is found
     */
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public GoogleBucketResource retrieveBucketById(UUID bucketResourceId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", bucketResourceId);
        GoogleBucketResource bucketResource = retrieveBucketBy(sqlBucketRetrievedById, params);
        if (bucketResource == null) {
            throw new GoogleResourceNotFoundException("Bucket not found for id:" + bucketResourceId);
        }
        return bucketResource;
    }

    /**
     * Delete the bucket_resource metadata row associated with the bucket, provided the row is either unlocked or
     * locked by the provided flight.
     *
     * @param bucketName name of bucket to delete
     * @param flightId flight trying to do the delete
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

    private GoogleBucketResource retrieveBucketBy(String sql, MapSqlParameterSource params) {
        List<GoogleBucketResource> bucketResources =
            jdbcTemplate.query(sql, params, (rs, rowNum) -> {
                // Make project resource and a bucket resource from the query result
                GoogleProjectResource projectResource = new GoogleProjectResource()
                    .id(rs.getObject("project_resource_id", UUID.class))
                    .googleProjectId(rs.getString("google_project_id"))
                    .googleProjectNumber(rs.getString("google_project_number"))
                    .profileId(rs.getObject("profile_id", UUID.class));

                String region = rs.getString("region");

                // Since storing the region was not in the original data, we supply the
                // default if a value is not present.
                return new GoogleBucketResource()
                    .projectResource(projectResource)
                    .resourceId(rs.getObject("bucket_resource_id", UUID.class))
                    .name(rs.getString("name"))
                    .flightId(rs.getString("flightid"))
                    .region(region == null ? defaultRegion : region);
            });

        if (bucketResources.size() > 1) {
            throw new CorruptMetadataException("Found more than one result for bucket resource: " +
                bucketResources.get(0).getName());
        }

        return (bucketResources.size() == 0 ? null : bucketResources.get(0));
    }

}

