package bio.terra.service.snapshot;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.DaoUtils.UuidMapper;
import bio.terra.common.MetadataEnumeration;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.exception.MissingRowCountsException;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class SnapshotDao {

  private final Logger logger = LoggerFactory.getLogger(SnapshotDao.class);

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final SnapshotTableDao snapshotTableDao;
  private final SnapshotMapTableDao snapshotMapTableDao;
  private final SnapshotRelationshipDao snapshotRelationshipDao;
  private final DatasetDao datasetDao;
  private final ResourceService resourceService;
  private final ObjectMapper objectMapper;

  private static final String TABLE_NAME = "snapshot";

  private static final String snapshotSourceStorageQuery =
      "(SELECT jsonb_agg(sr) "
          + "FROM (SELECT region, cloud_resource as \"cloudResource\", "
          + "lower(cloud_platform) as \"cloudPlatform\", dataset_id as \"datasetId\" "
          + "FROM storage_resource "
          + "WHERE dataset_id = snapshot_source.dataset_id) sr) AS storage ";

  private static final String summaryCloudPlatformQuery =
      "(SELECT pr.google_project_id "
          + "  FROM project_resource pr "
          + "  WHERE pr.id = snapshot.project_resource_id) as google_project_id, "
          + "(SELECT sar.name "
          + "  FROM storage_account_resource sar "
          + "  WHERE sar.id = snapshot.storage_account_resource_id) as storage_account_name, ";

  @Autowired
  public SnapshotDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      SnapshotTableDao snapshotTableDao,
      SnapshotMapTableDao snapshotMapTableDao,
      SnapshotRelationshipDao snapshotRelationshipDao,
      DatasetDao datasetDao,
      ResourceService resourceService,
      ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.snapshotTableDao = snapshotTableDao;
    this.snapshotMapTableDao = snapshotMapTableDao;
    this.snapshotRelationshipDao = snapshotRelationshipDao;
    this.datasetDao = datasetDao;
    this.resourceService = resourceService;
    this.objectMapper = objectMapper;
  }

  /**
   * Lock the snapshot object before doing something with it (e.g. delete). This method returns
   * successfully when there is a snapshot object locked by this flight, and throws an exception in
   * all other cases. So, multiple locks can succeed with no errors. Logic flow of the method: 1.
   * Update the snapshot record to give this flight the lock. 2. Throw an exception if no records
   * were updated.
   *
   * @param snapshotId id of the snapshot to lock, this is a unique column
   * @param flightId flight id that wants to lock the snapshot
   * @throws SnapshotLockException if the snapshot is locked by another flight
   * @throws SnapshotNotFoundException if the snapshot does not exist
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void lock(UUID snapshotId, String flightId) {
    if (flightId == null) {
      throw new SnapshotLockException("Locking flight id cannot be null");
    }

    // update the snapshot entry and lock it by setting the flight id
    String sql =
        "UPDATE snapshot SET flightid = :flightid "
            + "WHERE id = :id AND (flightid IS NULL OR flightid = :flightid)";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("id", snapshotId).addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);

    // if no rows were updated, then throw an exception
    if (numRowsUpdated == 0) {
      // this method checks if the snapshot exists
      // if it does not exist, then the method throws a SnapshotNotFoundException
      // we don't need the result (snapshot summary) here, just the existence check, so ignore the
      // return value.
      retrieveSummaryById(snapshotId);

      // otherwise, throw a Lock exception
      logger.debug("numRowsUpdated=" + numRowsUpdated);
      throw new SnapshotLockException("Failed to lock the snapshot");
    }
  }

  /**
   * Unlock the snapshot object when finished doing something with it (e.g. delete). If the snapshot
   * is not locked by this flight, then the method is a no-op. It does not throw an exception in
   * this case. So, multiple unlocks can succeed with no errors. The method does return a boolean
   * indicating whether any rows were updated or not. So, callers can decide to throw an error if
   * the unlock was a no-op.
   *
   * @param snapshotId id of the snapshot to unlock, this is a unique column
   * @param flightId flight id that wants to unlock the snapshot
   * @return true if a snapshot was unlocked, false otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean unlock(UUID snapshotId, String flightId) {
    // update the snapshot entry to remove the flightid IF it is currently set to this flightid
    String sql = "UPDATE snapshot SET flightid = NULL " + "WHERE id = :id AND flightid = :flightid";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("id", snapshotId).addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);
    logger.debug("numRowsUpdated=" + numRowsUpdated);
    return (numRowsUpdated == 1);
  }

  /**
   * Create a new snapshot object and lock it. An exception is thrown if the snapshot already
   * exists. The correct order to call the SnapshotDao methods when creating a snapshot is:
   * createAndLock, unlock.
   *
   * @param snapshot the snapshot object to create
   * @return the id of the new snapshot
   * @throws InvalidSnapshotException if a row already exists with this snapshot name
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createAndLock(Snapshot snapshot, String flightId) {
    logger.debug("createAndLock snapshot " + snapshot.getName());

    String sql =
        "INSERT INTO snapshot (name, description, profile_id, project_resource_id, id, consent_code, flightid, creation_information, properties) "
            + "VALUES (:name, :description, :profile_id, :project_resource_id, :id, :consent_code, :flightid, :creation_information::jsonb, :properties::jsonb) ";
    String creationInfo;
    try {
      creationInfo = objectMapper.writeValueAsString(snapshot.getCreationInformation());
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Invalid JSON in snapshot creationInformation, we should've caught this already", e);
    }
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", snapshot.getName())
            .addValue("description", snapshot.getDescription())
            .addValue("profile_id", snapshot.getProfileId())
            .addValue("project_resource_id", snapshot.getProjectResourceId())
            .addValue("id", snapshot.getId())
            .addValue("consent_code", snapshot.getConsentCode())
            .addValue("flightid", flightId)
            .addValue("creation_information", creationInfo)
            .addValue(
                "properties", DaoUtils.propertiesToString(objectMapper, snapshot.getProperties()));
    try {
      jdbcTemplate.update(sql, params);
    } catch (DuplicateKeyException dkEx) {
      throw new InvalidSnapshotException(
          "Snapshot name or id already exists: " + snapshot.getName() + ", " + snapshot.getId(),
          dkEx);
    }

    snapshotTableDao.createTables(snapshot.getId(), snapshot.getTables());
    for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
      createSnapshotSource(snapshotSource);
    }
  }

  /**
   * This method is protected because it's for use in tests only. Currently, we don't expose the
   * lock state of a snapshot outside of the DAO for other API code to consume.
   *
   * @param id
   * @return the flightid that holds an exclusive lock. null if none.
   */
  protected String getExclusiveLockState(UUID id) {
    try {
      String sql = "SELECT flightid FROM snapshot WHERE id = :id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(sql, params, String.class);
    } catch (EmptyResultDataAccessException ex) {
      throw new SnapshotNotFoundException("Snapshot not found for id " + id);
    }
  }

  private void createSnapshotSource(SnapshotSource snapshotSource) {
    String sql =
        "INSERT INTO snapshot_source (snapshot_id, dataset_id, asset_id)"
            + " VALUES (:snapshot_id, :dataset_id, :asset_id)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("snapshot_id", snapshotSource.getSnapshot().getId())
            .addValue("dataset_id", snapshotSource.getDataset().getId());
    if (snapshotSource.getAssetSpecification() != null) {
      params.addValue("asset_id", snapshotSource.getAssetSpecification().getId());
    } else {
      params.addValue("asset_id", null);
    }
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    jdbcTemplate.update(sql, params, keyHolder);
    UUID id = keyHolder.getId();
    snapshotSource.id(id);
    snapshotMapTableDao.createTables(id, snapshotSource.getSnapshotMapTables());
    snapshotRelationshipDao.createSnapshotRelationships(snapshotSource.getSnapshot());
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean delete(UUID id) {
    logger.debug("delete snapshot by id: " + id);
    int rowsAffected =
        jdbcTemplate.update(
            "DELETE FROM snapshot WHERE id = :id", new MapSqlParameterSource().addValue("id", id));
    return rowsAffected > 0;
  }

  /**
   * This is a convenience wrapper that returns a snapshot only if it is NOT exclusively locked.
   * This method is intended for user-facing API calls (e.g. from RepositoryApiController).
   *
   * @param snapshotId the snapshot id
   * @return the Snapshot object
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public Snapshot retrieveAvailableSnapshot(UUID snapshotId) {
    return retrieveSnapshot(snapshotId, true);
  }

  /**
   * This is a convenience wrapper that returns a snapshot project only if it is NOT exclusively
   * locked. This method is intended for user-facing API calls (e.g. from RepositoryApiController).
   *
   * @param snapshotId the snapshot id
   * @return the SnapshotProject object
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public SnapshotProject retrieveAvailableSnapshotProject(UUID snapshotId) {
    return retrieveSnapshotProject(snapshotId, true);
  }

  /**
   * This is a convenience wrapper that returns a snapshot, regardless of whether it is exclusively
   * locked. Most places in the API code that are retrieving a snapshot will call this method.
   *
   * @param snapshotId the snapshot id
   * @return the Snapshot object
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public Snapshot retrieveSnapshot(UUID snapshotId) {
    return retrieveSnapshot(snapshotId, false);
  }

  /**
   * Retrieves a Snapshot object from the snapshot id.
   *
   * @param snapshotId the snapshot id
   * @param onlyRetrieveAvailable true to exclude snapshots that are exclusively locked, false to
   *     include all snapshots
   * @return the Snapshot object
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public Snapshot retrieveSnapshot(UUID snapshotId, boolean onlyRetrieveAvailable) {
    logger.info("retrieve snapshot id: " + snapshotId);
    String sql = "SELECT * FROM snapshot WHERE id = :id";
    if (onlyRetrieveAvailable) { // exclude snapshots that are exclusively locked
      sql += " AND flightid IS NULL";
    }
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", snapshotId);
    Snapshot snapshot = retrieveWorker(sql, params);
    if (snapshot == null) {
      throw new SnapshotNotFoundException("Snapshot not found - id: " + snapshotId);
    }
    return snapshot;
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public SnapshotProject retrieveSnapshotProject(UUID snapshotId, boolean onlyRetrieveAvailable) {
    logger.debug("retrieve snapshot id: " + snapshotId);
    String sql =
        "SELECT snapshot.id, name, snapshot.profile_id, google_project_id, "
            // Select the source dataset project information
            + "(SELECT jsonb_agg(ds)\n"
            + "  FROM (SELECT dataset.id, dataset.name, p.profile_id as \"profileId\", p.google_project_id as \"dataProject\"\n"
            + "        FROM snapshot_source ss"
            + "        JOIN dataset ON ss.dataset_id = dataset.id"
            + "        LEFT JOIN project_resource p ON dataset.project_resource_id = p.id"
            + "        WHERE snapshot.id = ss.snapshot_id) ds)"
            + "  AS dataset_sources, "
            // Detect the cloud provider of the snapshot
            + "case "
            + "when storage_account_resource_id is not null then 'azure' "
            + "when project_resource_id is not null then 'gcp' "
            + "end AS cloud_platform "
            + "FROM snapshot "
            + "LEFT JOIN project_resource ON snapshot.project_resource_id = project_resource.id "
            + "WHERE snapshot.id = :id";
    if (onlyRetrieveAvailable) { // exclude snapshots that are exclusively locked
      sql += " AND flightid IS NULL";
    }
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", snapshotId);
    SnapshotProject snapshotProject = retrieveSnapshotProject(sql, params);
    if (snapshotProject == null) {
      throw new SnapshotNotFoundException("Snapshot not found - id: " + snapshotId);
    }
    return snapshotProject;
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public Snapshot retrieveSnapshotByName(String name) {
    String sql = "SELECT * FROM snapshot WHERE name = :name";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
    Snapshot snapshot = retrieveWorker(sql, params);
    if (snapshot == null) {
      throw new SnapshotNotFoundException("Snapshot not found - name: '" + name + "'");
    }
    return snapshot;
  }

  private SnapshotRequestContentsModel stringToSnapshotRequestContentsModel(String json) {
    try {
      return objectMapper.readValue(json, SnapshotRequestContentsModel.class);
    } catch (JsonProcessingException e) {
      logger.warn("Error parsing creation_information into SnapshotRequestContentsModel", e);
      return null;
    }
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  Snapshot retrieveWorker(String sql, MapSqlParameterSource params) {
    try {
      Snapshot snapshot =
          jdbcTemplate.queryForObject(
              sql,
              params,
              (rs, rowNum) ->
                  new Snapshot()
                      .id(rs.getObject("id", UUID.class))
                      .name(rs.getString("name"))
                      .description(rs.getString("description"))
                      .createdDate(rs.getTimestamp("created_date").toInstant())
                      .profileId(rs.getObject("profile_id", UUID.class))
                      .projectResourceId(rs.getObject("project_resource_id", UUID.class))
                      .creationInformation(
                          stringToSnapshotRequestContentsModel(
                              rs.getString("creation_information")))
                      .consentCode(rs.getString("consent_code"))
                      .properties(
                          DaoUtils.stringToProperties(objectMapper, rs.getString("properties"))));
      // needed for findbugs. but really can't be null
      if (snapshot != null) {
        // retrieve the snapshot tables and relationships
        snapshot.snapshotTables(snapshotTableDao.retrieveTables(snapshot.getId()));
        snapshotRelationshipDao.retrieve(snapshot);

        // Must be done after we make the snapshot tables so that we can resolve the table
        // and column references
        snapshot.snapshotSources(retrieveSnapshotSources(snapshot));

        // Retrieve the project resource associated with the snapshot
        // This is a bit sketchy filling in the object via a dao in another package.
        // It seemed like the cleanest thing to me at the time.
        UUID projectResourceId = snapshot.getProjectResourceId();
        if (projectResourceId != null) {
          snapshot.projectResource(
              resourceService.getProjectResource(snapshot.getProjectResourceId()));
        }

        // Retrieve the Azure Storage Account associated with the snapshot.
        resourceService
            .getSnapshotStorageAccount(snapshot.getId())
            .ifPresent(snapshot::storageAccountResource);
      }
      return snapshot;
    } catch (EmptyResultDataAccessException ex) {
      return null;
    }
  }

  private SnapshotProject retrieveSnapshotProject(String sql, MapSqlParameterSource params) {
    try {
      SnapshotProject snapshotProject =
          jdbcTemplate.queryForObject(
              sql,
              params,
              (rs, rowNum) -> {
                List<DatasetProject> datasetProjects;
                try {
                  datasetProjects =
                      objectMapper.readValue(
                          rs.getString("dataset_sources"), new TypeReference<>() {});
                } catch (JsonProcessingException e) {
                  throw new CorruptMetadataException("Invalid dataset sources for snapshot");
                }
                return new SnapshotProject()
                    .id(rs.getObject("id", UUID.class))
                    .name(rs.getString("name"))
                    .profileId(rs.getObject("profile_id", UUID.class))
                    .dataProject(rs.getString("google_project_id"))
                    .cloudPlatform(CloudPlatform.fromValue(rs.getString("cloud_platform")))
                    .sourceDatasetProjects(datasetProjects);
              });
      return snapshotProject;
    } catch (EmptyResultDataAccessException ex) {
      return null;
    }
  }

  private List<SnapshotSource> retrieveSnapshotSources(Snapshot snapshot) {
    // We collect all of the source ids first to avoid introducing a recursive query. While the
    // recursive
    // query might work, it makes debugging errors more difficult.
    class RawSourceData {
      private UUID id;
      private UUID datasetId;
      private UUID assetId;
    }

    String sql =
        "SELECT id, dataset_id, asset_id FROM snapshot_source WHERE snapshot_id = :snapshot_id";
    List<RawSourceData> rawList =
        jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("snapshot_id", snapshot.getId()),
            (rs, rowNum) -> {
              RawSourceData raw = new RawSourceData();
              raw.id = UUID.fromString(rs.getString("id"));
              raw.datasetId = rs.getObject("dataset_id", UUID.class);
              raw.assetId = rs.getObject("asset_id", UUID.class);
              return raw;
            });

    List<SnapshotSource> snapshotSources = new ArrayList<>();
    for (RawSourceData raw : rawList) {
      Dataset dataset = datasetDao.retrieve(raw.datasetId);
      SnapshotSource snapshotSource =
          new SnapshotSource().id(raw.id).snapshot(snapshot).dataset(dataset);

      if (raw.assetId != null) { // if there is no assetId, then dont check for a spec
        // Find the matching asset in the dataset
        Optional<AssetSpecification> assetSpecification =
            dataset.getAssetSpecificationById(raw.assetId);
        if (!assetSpecification.isPresent()) {
          throw new CorruptMetadataException("Asset referenced by snapshot source was not found!");
        }
        snapshotSource.assetSpecification(assetSpecification.get());
      }

      // Now that we have access to all of the parts, build the map structure
      snapshotSource.snapshotMapTables(
          snapshotMapTableDao.retrieveMapTables(snapshot, snapshotSource));

      snapshotSources.add(snapshotSource);
    }

    return snapshotSources;
  }

  /**
   * @param permissions RAS dbGaP permissions held by the caller (derived from the caller's linked
   *     RAS passports)
   * @return snapshot UUIDs accessible under the permissions
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<UUID> getAccessibleSnapshots(List<RasDbgapPermissions> permissions) {
    String sql =
        "SELECT snapshot.id FROM snapshot "
            + "JOIN snapshot_source ON snapshot.id = snapshot_source.snapshot_id "
            + "JOIN dataset ON dataset.id = snapshot_source.dataset_id "
            + "WHERE (snapshot.consent_code, dataset.phs_id) IN (:permissions)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(
                "permissions",
                permissions.stream()
                    .map(c -> new String[] {c.consent_group(), c.phs_id()})
                    .toList());
    return jdbcTemplate.query(sql, params, new UuidMapper("id"));
  }

  /**
   * Fetch a list of all the available snapshots. This method returns summary objects, which do not
   * include sub-objects associated with snapshots (e.g. tables). Note that this method will only
   * return snapshots that are NOT exclusively locked.
   *
   * @param offset skip this many snapshots from the beginning of the list (intended for "scrolling"
   *     behavior)
   * @param limit only return this many snapshots in the list
   * @param sort field for order by clause. possible values are: name, description, created_date
   * @param direction asc or desc
   * @param filter string to match (SQL ILIKE) in snapshots name or description
   * @param accessibleSnapshotIds list of snapshots ids that caller has access to (fetched from IAM
   *     service)
   * @return a list of dataset summary objects
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public MetadataEnumeration<SnapshotSummary> retrieveSnapshots(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      List<UUID> datasetIds,
      Collection<UUID> accessibleSnapshotIds) {
    logger.debug(
        "retrieve snapshots offset: "
            + offset
            + " limit: "
            + limit
            + " sort: "
            + sort
            + " direction: "
            + direction
            + " filter: "
            + filter
            + " datasetIds: "
            + StringUtils.join(datasetIds, ","));
    MapSqlParameterSource params = new MapSqlParameterSource();
    List<String> whereClauses = new ArrayList<>();
    DaoUtils.addAuthzIdsClause(accessibleSnapshotIds, params, whereClauses, TABLE_NAME);
    whereClauses.add(" snapshot.flightid IS NULL"); // exclude snapshots that are exclusively locked
    String joinSql =
        " JOIN snapshot_source ON snapshot.id = snapshot_source.snapshot_id "
            + "JOIN dataset on dataset.id = snapshot_source.dataset_id ";

    if (!datasetIds.isEmpty()) {
      String datasetMatchSql = "snapshot_source.dataset_id IN (:datasetIds)";
      whereClauses.add(datasetMatchSql);
      params.addValue("datasetIds", datasetIds);
    }

    // get filtered total count of objects
    String countSql =
        "SELECT count(snapshot.id) AS total FROM snapshot "
            + joinSql
            + " WHERE "
            + StringUtils.join(whereClauses, " AND ");
    Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);
    if (total == null) {
      throw new CorruptMetadataException("Impossible null value from total count");
    }

    // add the filter to the clause to get the actual items
    DaoUtils.addFilterClause(filter, params, whereClauses, TABLE_NAME);
    DaoUtils.addRegionFilterClause(region, params, whereClauses, "snapshot_source.dataset_id");

    String whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");

    // get filtered total count of objects
    String filteredCountSql =
        "SELECT count(snapshot.id) AS total FROM snapshot "
            + joinSql
            + " WHERE "
            + StringUtils.join(whereClauses, " AND ");
    Integer filteredTotal = jdbcTemplate.queryForObject(filteredCountSql, params, Integer.class);
    if (filteredTotal == null) {
      throw new CorruptMetadataException("Impossible null value from filtered count");
    }

    String sql =
        "SELECT snapshot.id, snapshot.name, snapshot.description, snapshot.created_date, snapshot.profile_id, "
            + "snapshot_source.id, dataset.secure_monitoring, snapshot.consent_code, dataset.phs_id, dataset.self_hosted,"
            + summaryCloudPlatformQuery
            + snapshotSourceStorageQuery
            + "FROM snapshot "
            + joinSql
            + whereSql
            + DaoUtils.orderByClause(sort, direction, TABLE_NAME)
            + " OFFSET :offset LIMIT :limit";

    params.addValue("offset", offset).addValue("limit", limit);
    List<SnapshotSummary> summaries = jdbcTemplate.query(sql, params, new SnapshotSummaryMapper());

    return new MetadataEnumeration<SnapshotSummary>()
        .items(summaries)
        .total(total)
        .filteredTotal(filteredTotal);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public SnapshotSummary retrieveSummaryById(UUID id) {
    logger.debug("retrieve snapshot summary for id: " + id);
    try {
      String sql =
          "SELECT snapshot.*, dataset.secure_monitoring, dataset.phs_id, dataset.self_hosted,"
              + summaryCloudPlatformQuery
              + snapshotSourceStorageQuery
              + "FROM snapshot "
              + "JOIN snapshot_source ON snapshot.id = snapshot_source.snapshot_id "
              + "JOIN dataset ON dataset.id = snapshot_source.dataset_id "
              + "WHERE snapshot.id = :id ";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(sql, params, new SnapshotSummaryMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new SnapshotNotFoundException("Snapshot not found - id: " + id);
    }
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<SnapshotSummary> retrieveSnapshotsForDataset(UUID datasetId) {
    try {
      String sql =
          "SELECT snapshot.id, snapshot.name, snapshot.description, snapshot.created_date, snapshot.profile_id, "
              + "dataset.secure_monitoring, snapshot.consent_code, dataset.phs_id, dataset.self_hosted,"
              + summaryCloudPlatformQuery
              + snapshotSourceStorageQuery
              + "FROM snapshot "
              + "JOIN snapshot_source ON snapshot.id = snapshot_source.snapshot_id "
              + "JOIN dataset ON dataset.id = snapshot_source.dataset_id "
              + "WHERE snapshot_source.dataset_id = :datasetId";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("datasetId", datasetId);
      return jdbcTemplate.query(sql, params, new SnapshotSummaryMapper());
    } catch (EmptyResultDataAccessException ex) {
      // this is ok - used during dataset delete to validate no snapshots reference the dataset
      return Collections.emptyList();
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateSnapshotTableRowCounts(Snapshot snapshot, Map<String, Long> tableRowCounts) {
    String sql =
        "UPDATE snapshot_table SET row_count = :rowCount "
            + "WHERE parent_id = :snapshotId AND name = :tableName";
    for (SnapshotTable snapshotTable : snapshot.getTables()) {
      String tableName = snapshotTable.getName();
      if (!tableRowCounts.containsKey(tableName)) {
        throw new MissingRowCountsException("Missing counts for " + tableName);
      }
      MapSqlParameterSource params =
          new MapSqlParameterSource()
              .addValue("rowCount", tableRowCounts.get(tableName))
              .addValue("snapshotId", snapshot.getId())
              .addValue("tableName", tableName);
      jdbcTemplate.update(sql, params);
    }
  }

  /**
   * Update a snapshot according to specified fields in the patch request. Any fields unspecified in
   * the request will remain unaltered.
   *
   * @param id snapshot UUID
   * @param patchRequest updates to merge with existing snapshot
   * @return whether the snapshot record was updated
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean patch(UUID id, SnapshotPatchRequestModel patchRequest) {
    String sql =
        "UPDATE snapshot SET consent_code = COALESCE(:consent_code, consent_code), "
            + "properties = COALESCE(:properties::jsonb, properties) WHERE id = :id";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("consent_code", patchRequest.getConsentCode())
            .addValue("id", id)
            .addValue(
                "properties",
                DaoUtils.propertiesToString(objectMapper, patchRequest.getProperties()));

    int rowsAffected = jdbcTemplate.update(sql, params);
    boolean patchSucceeded = (rowsAffected == 1);

    if (patchSucceeded) {
      logger.info("Snapshot {} patched with {}", id, patchRequest.toString());
    }
    return patchSucceeded;
  }

  private class SnapshotSummaryMapper implements RowMapper<SnapshotSummary> {

    public SnapshotSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
      String snapshotId = rs.getString("id");
      List<StorageResource> storageResources;
      try {
        storageResources =
            objectMapper.readValue(rs.getString("storage"), new TypeReference<>() {});
      } catch (JsonProcessingException e) {
        throw new CorruptMetadataException(
            String.format("Invalid storage for snapshot - id: %s", snapshotId));
      }

      boolean isAzure =
          storageResources.stream()
              .anyMatch(sr -> CloudPlatformWrapper.of(sr.getCloudPlatform()).isAzure());

      CloudPlatform snapshotCloudPlatform = isAzure ? CloudPlatform.AZURE : CloudPlatform.GCP;

      return new SnapshotSummary()
          .id(UUID.fromString(snapshotId))
          .name(rs.getString("name"))
          .description(rs.getString("description"))
          .createdDate(rs.getTimestamp("created_date").toInstant())
          .profileId(rs.getObject("profile_id", UUID.class))
          .storage(storageResources)
          .secureMonitoringEnabled(rs.getBoolean("secure_monitoring"))
          .cloudPlatform(snapshotCloudPlatform)
          .dataProject(rs.getString("google_project_id"))
          .storageAccount(rs.getString("storage_account_name"))
          .consentCode(rs.getString("consent_code"))
          .phsId(rs.getString("phs_id"))
          .selfHosted(rs.getBoolean("self_hosted"));
    }
  }
}
