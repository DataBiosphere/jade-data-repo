package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.DaoKeyHolder;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.profile.exception.ProfileInUseException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class GoogleResourceDao {

  private static final Logger logger = LoggerFactory.getLogger(GoogleResourceDao.class);
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final GoogleResourceConfiguration googleResourceConfiguration;
  private final GoogleRegion defaultRegion;
  private final String tdrServiceAccountEmail;

  private static final String sqlProjectRetrieve =
      "SELECT id, google_project_id, google_project_number, profile_id, service_account"
          + " FROM project_resource";
  private static final String sqlProjectRetrieveById =
      sqlProjectRetrieve + " WHERE marked_for_delete = false AND id = :id";
  private static final String sqlProjectRetrieveByProjectId =
      sqlProjectRetrieve
          + " WHERE marked_for_delete = false AND google_project_id = :google_project_id";
  private static final String sqlProjectRetrieveByIdForDelete =
      sqlProjectRetrieve + " WHERE marked_for_delete = true AND id = :id";
  private static final String sqlProjectRetrieveByBillingProfileId =
      sqlProjectRetrieve + " WHERE marked_for_delete = false AND profile_id = :profile_id";

  private static final String sqlBucketRetrieve =
      "SELECT distinct p.id AS project_resource_id, google_project_id, google_project_number, profile_id,"
          + " b.id AS bucket_resource_id, name, sr.region as region, flightid, b.autoclass_enabled "
          + "FROM bucket_resource b "
          + "JOIN project_resource p ON b.project_resource_id = p.id "
          + "LEFT JOIN dataset_bucket db on b.id = db.bucket_resource_id "
          + "LEFT JOIN storage_resource sr on db.dataset_id = sr.dataset_id AND sr.cloud_resource = 'BUCKET' "
          + "WHERE b.marked_for_delete = false";
  private static final String sqlBucketRetrievedById = sqlBucketRetrieve + " AND b.id = :id";
  private static final String sqlBucketRetrievedByName = sqlBucketRetrieve + " AND b.name = :name";

  // Check if project is used by any resource
  private static final String sqlProjectRefs =
      "SELECT pid, dscnt + sncnt + bkcnt AS refcnt FROM "
          + " (SELECT"
          + "  project_resource.id AS pid,"
          + "  (SELECT COUNT(*) FROM dataset WHERE dataset.project_resource_id = project_resource.id) AS dscnt,"
          + "  (SELECT COUNT(*) FROM snapshot WHERE snapshot.project_resource_id = project_resource.id) AS sncnt,"
          + "  (SELECT count(*) FROM bucket_resource, dataset_bucket"
          + "    WHERE bucket_resource.project_resource_id = project_resource.id"
          + "    AND bucket_resource.id = dataset_bucket.bucket_resource_id"
          + "    AND dataset_bucket.successful_ingests > 0) AS bkcnt"
          + " FROM project_resource";

  // Given a profile id, compute the count of all references to projects associated with the profile
  private static final String sqlProfileProjectRefs =
      sqlProjectRefs + " WHERE project_resource.profile_id = :profile_id) AS X";

  // Given a list of projects, compute the count of all references to project
  private static final String sqlProjectListRefs =
      sqlProjectRefs + " WHERE project_resource.id IN (:project_ids)) AS X";

  // Class for collecting results from the above query
  private static class ProjectRefs {

    private UUID projectId;
    private long refCount;

    public UUID getProjectId() {
      return projectId;
    }

    public ProjectRefs projectId(UUID projectId) {
      this.projectId = projectId;
      return this;
    }

    public long getRefCount() {
      return refCount;
    }

    public ProjectRefs refCount(long refCount) {
      this.refCount = refCount;
      return this;
    }
  }

  @Autowired
  public GoogleResourceDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      GcsConfiguration gcsConfiguration,
      GoogleResourceConfiguration googleResourceConfiguration,
      @Qualifier("tdrServiceAccountEmail") String tdrServiceAccountEmail)
      throws SQLException {
    this.jdbcTemplate = jdbcTemplate;
    this.defaultRegion = GoogleRegion.fromValue(gcsConfiguration.getRegion());
    this.googleResourceConfiguration = googleResourceConfiguration;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  // -- project resource methods --

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public UUID createProject(GoogleProjectResource project) {
    String sql =
        "INSERT INTO project_resource "
            + "(google_project_id, google_project_number, profile_id) VALUES "
            + "(:google_project_id, :google_project_number, :profile_id)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
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

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public Optional<GoogleProjectResource> retrieveProjectByGoogleProjectIdMaybe(
      String googleProjectId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("google_project_id", googleProjectId);
    return Optional.ofNullable(retrieveProjectBy(sqlProjectRetrieveByProjectId, params, false));
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public GoogleProjectResource retrieveProjectByIdForDelete(UUID id) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    return retrieveProjectBy(sqlProjectRetrieveByIdForDelete, params);
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<GoogleProjectResource> retrieveProjectsByBillingProfileId(UUID billingProfileId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("profile_id", billingProfileId);
    return retrieveProjectListBy(sqlProjectRetrieveByBillingProfileId, params);
  }

  // NOTE: This method is currently only used from tests
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteProject(UUID id) {
    String sql = "DELETE FROM project_resource WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    int rowsAffected = jdbcTemplate.update(sql, params);
    logger.info("Project resource {} was {}", id, (rowsAffected > 0 ? "deleted" : "not found"));
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public List<UUID> markUnusedProjectsForDelete(List<UUID> projectResourceIds) {
    // Check if any of the projects are in use
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("project_ids", projectResourceIds);
    List<ProjectRefs> projectRefs =
        jdbcTemplate.query(
            sqlProjectListRefs,
            params,
            (rs, rowNum) ->
                new ProjectRefs()
                    .projectId(rs.getObject("pid", UUID.class))
                    .refCount(rs.getLong("refcnt")));

    List<UUID> projectsToDelete =
        projectRefs.stream()
            .filter(projectRef -> projectRef.refCount == 0)
            .map(projectRef -> projectRef.projectId)
            .collect(Collectors.toList());
    // mark those that are not in use for delete
    markProjectsForDelete(projectsToDelete);

    // return only the projects for delete
    return projectsToDelete;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    // Collect all projects related to the incoming profile and compute the number of references
    // on those projects. Note that so long as we use the one project profile data location selector
    // there will never be more than one project. The code will support the case where that
    // relationship
    // is different.
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("profile_id", profileId);
    List<ProjectRefs> projectRefs =
        jdbcTemplate.query(
            sqlProfileProjectRefs,
            params,
            (rs, rowNum) ->
                new ProjectRefs()
                    .projectId(rs.getObject("pid", UUID.class))
                    .refCount(rs.getLong("refcnt")));

    // If the profile is in use by any project, we bail here.
    long totalRefs = 0;
    for (ProjectRefs ref : projectRefs) {
      logger.info(
          "Profile project reference projectId: {} refCount: {}",
          ref.getProjectId(),
          ref.getRefCount());
      totalRefs += ref.getRefCount();
    }

    // long totalRefs = projectRefs.stream().mapToLong(ProjectRefs::getRefCount).sum();
    logger.info(
        "Profile {} has {} projects with the total of {} references",
        profileId,
        projectRefs.size(),
        totalRefs);

    if (totalRefs > 0) {
      throw new ProfileInUseException("Profile is in use and cannot be deleted");
    }

    // Common variables for marking projects and buckets for delete.
    List<UUID> projectIds =
        projectRefs.stream().map(ProjectRefs::getProjectId).collect(Collectors.toList());
    markProjectsForDelete(projectIds);

    return projectIds;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void markProjectsForDelete(List<UUID> projectIds) {
    if (!projectIds.isEmpty()) {
      MapSqlParameterSource markParams =
          new MapSqlParameterSource().addValue("project_ids", projectIds);

      final String sqlMarkProjects =
          "UPDATE project_resource SET marked_for_delete = true WHERE id IN (:project_ids)";
      jdbcTemplate.update(sqlMarkProjects, markParams);

      final String sqlMarkBuckets =
          "UPDATE bucket_resource SET marked_for_delete = true WHERE project_resource_id IN (:project_ids)";
      jdbcTemplate.update(sqlMarkBuckets, markParams);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateProjectResourceServiceAccount(UUID projectId, String serviceAccountEmail) {
    MapSqlParameterSource markParams =
        new MapSqlParameterSource()
            .addValue("project_id", projectId)
            .addValue("service_account", serviceAccountEmail);

    final String sqlMarkProjects =
        "UPDATE project_resource SET service_account = :service_account WHERE id = :project_id";
    jdbcTemplate.update(sqlMarkProjects, markParams);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteProjectMetadata(List<UUID> projectIds) {
    if (!projectIds.isEmpty()) {
      MapSqlParameterSource markParams =
          new MapSqlParameterSource().addValue("project_ids", projectIds);

      // Delete the buckets
      final String sqlMarkBuckets =
          "DELETE FROM bucket_resource WHERE project_resource_id IN (:project_ids)";
      jdbcTemplate.update(sqlMarkBuckets, markParams);

      // Delete the projects
      final String sqlMarkProjects = "DELETE FROM project_resource WHERE id IN (:project_ids)";
      jdbcTemplate.update(sqlMarkProjects, markParams);
    }
  }

  private GoogleProjectResource retrieveProjectBy(String sql, MapSqlParameterSource params) {
    return retrieveProjectBy(sql, params, true);
  }

  private GoogleProjectResource retrieveProjectBy(
      String sql, MapSqlParameterSource params, boolean failIfNotFound) {
    List<GoogleProjectResource> projectList = retrieveProjectListBy(sql, params);
    if (projectList.size() == 0) {
      if (!failIfNotFound) {
        return null;
      }
      throw new GoogleResourceNotFoundException("Project not found");
    }
    if (projectList.size() > 1) {
      throw new CorruptMetadataException(
          "Found more than one result for project resource: "
              + projectList.get(0).getGoogleProjectId());
    }
    return projectList.get(0);
  }

  private List<GoogleProjectResource> retrieveProjectListBy(
      String sql, MapSqlParameterSource params) {
    return jdbcTemplate.query(
        sql,
        params,
        (rs, rowNum) -> {
          String serviceAccount = rs.getString("service_account");
          return new GoogleProjectResource()
              .id(rs.getObject("id", UUID.class))
              .profileId(rs.getObject("profile_id", UUID.class))
              .googleProjectId(rs.getString("google_project_id"))
              .googleProjectNumber(rs.getString("google_project_number"))
              .serviceAccount(Optional.ofNullable(serviceAccount).orElse(tdrServiceAccountEmail))
              .dedicatedServiceAccount(
                  StringUtils.isNotEmpty(serviceAccount)
                      && !StringUtils.equalsIgnoreCase(serviceAccount, tdrServiceAccountEmail));
        });
  }

  // -- bucket resource methods --

  /**
   * Insert a new row into the bucket_resource metadata table and give the provided flight the lock
   * by setting the flightid column. If there already exists a row with this bucket name, return
   * null instead of throwing an exception. c * @return a GoogleBucketResource if the insert
   * succeeded, null otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public GoogleBucketResource createAndLockBucket(
      String bucketName,
      GoogleProjectResource projectResource,
      GoogleRegion region,
      String flightId,
      boolean autoclassEnabled) {
    // Put an end to serialization errors here. We only come through here if we really need to
    // create
    // the bucket, so this is not on the path of most bucket lookups.
    jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE bucket_resource IN EXCLUSIVE MODE");

    String sql =
        "INSERT INTO bucket_resource (project_resource_id, name, flightid, autoclass_enabled) VALUES "
            + "(:project_resource_id, :name, :flightid, :autoclass_enabled) "
            + "ON CONFLICT ON CONSTRAINT bucket_resource_name_key DO NOTHING";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("project_resource_id", projectResource.getId())
            .addValue("name", bucketName)
            .addValue("flightid", flightId)
            .addValue("autoclass_enabled", autoclassEnabled);
    DaoKeyHolder keyHolder = new DaoKeyHolder();

    int numRowsUpdated = jdbcTemplate.update(sql, params, keyHolder);
    if (numRowsUpdated == 1) {
      return new GoogleBucketResource()
          .resourceId(keyHolder.getId())
          .flightId(flightId)
          .profileId(projectResource.getProfileId())
          .projectResource(projectResource)
          .name(bucketName)
          .region(region)
          .autoclassEnabled(autoclassEnabled);
    } else {
      return null;
    }
  }

  /**
   * Unlock an existing bucket_resource metadata row, by setting flightid = NULL. Only the flight
   * that currently holds the lock can unlock the row. The lock may not be held - that is not an
   * error
   *
   * @param bucketName bucket to unlock
   * @param flightId flight trying to unlock it
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void unlockBucket(String bucketName, String flightId) {
    String sql =
        "UPDATE bucket_resource SET flightid = NULL "
            + "WHERE name = :name AND flightid = :flightid";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("name", bucketName).addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);
    logger.info("Bucket {} was {}", bucketName, (numRowsUpdated > 0 ? "unlocked" : "not locked"));
  }

  /**
   * Fetch an existing bucket_resource metadata row using the name amd project id. This method
   * expects that there is exactly one row matching the provided name and project id.
   *
   * @param bucketName name of the bucket
   * @param requestedProjectId projectId in which we are searching for the bucket
   * @return a reference to the bucket as a POJO GoogleBucketResource or null if not found
   * @throws GoogleResourceException if the bucket matches, but is in the wrong project
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
          String.format(
              "A bucket with this name already exists for a different project: %s, %s",
              bucketName, requestedProjectId));
    }

    return bucketResource;
  }

  /**
   * Fetch an existing bucket_resource metadata row using the id. This method expects that there is
   * exactly one row matching the provided resource id.
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
   * Delete the bucket_resource metadata row associated with the bucket, provided the row is either
   * unlocked or locked by the provided flight.
   *
   * @param bucketName name of bucket to delete
   * @param flightId flight trying to do the delete
   * @return true if a row is deleted, false otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean deleteBucketMetadata(String bucketName, String flightId) {
    String sql =
        "DELETE FROM bucket_resource "
            + "WHERE name = :name AND (flightid = :flightid OR flightid IS NULL)";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("name", bucketName).addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);
    return (numRowsUpdated == 1);
  }

  private GoogleBucketResource retrieveBucketBy(String sql, MapSqlParameterSource params) {
    List<GoogleBucketResource> bucketResources =
        jdbcTemplate.query(
            sql,
            params,
            (rs, rowNum) -> {
              // Make project resource and a bucket resource from the query result
              GoogleProjectResource projectResource =
                  new GoogleProjectResource()
                      .id(rs.getObject("project_resource_id", UUID.class))
                      .googleProjectId(rs.getString("google_project_id"))
                      .googleProjectNumber(rs.getString("google_project_number"))
                      .profileId(rs.getObject("profile_id", UUID.class));

              GoogleRegion region =
                  Optional.ofNullable(rs.getString("region"))
                      .map(GoogleRegion::valueOf)
                      .orElse(defaultRegion);

              // Since storing the region was not in the original data, we supply the
              // default if a value is not present.
              return new GoogleBucketResource()
                  .projectResource(projectResource)
                  .resourceId(rs.getObject("bucket_resource_id", UUID.class))
                  .name(rs.getString("name"))
                  .flightId(rs.getString("flightid"))
                  .region(region)
                  .autoclassEnabled(rs.getObject("autoclass_enabled", Boolean.class));
            });

    if (bucketResources.size() > 1) {
      // TODO This is only here because of the dev case. It should be removed when we start using
      // RBS in dev.
      if (googleResourceConfiguration.getAllowReuseExistingBuckets()) {
        return bucketResources.get(0).region(defaultRegion);
      }
      throw new CorruptMetadataException(
          "Found more than one result for bucket resource: " + bucketResources.get(0).getName());
    }

    return (bucketResources.size() == 0 ? null : bucketResources.get(0));
  }
}
