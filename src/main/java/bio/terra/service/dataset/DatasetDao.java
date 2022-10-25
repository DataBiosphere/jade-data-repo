package bio.terra.service.dataset;

import static bio.terra.common.DaoUtils.retryQuery;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.exception.RetryQueryException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.journal.JournalService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DatasetDao {

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final DatasetTableDao tableDao;
  private final DatasetRelationshipDao relationshipDao;
  private final AssetDao assetDao;
  private final ConfigurationService configurationService;
  private final ResourceService resourceService;
  private final StorageResourceDao storageResourceDao;
  private final JournalService journalService;
  private final ObjectMapper objectMapper;

  private static final Logger logger = LoggerFactory.getLogger(DatasetDao.class);

  public static final String TABLE_NAME = "dataset";

  private static final String summaryQueryColumns =
      " dataset.id, dataset.name, description, default_profile_id, project_resource_id, "
          + "dataset.application_resource_id, secure_monitoring, phs_id, self_hosted, "
          + "properties, created_date, ";

  private static final String summaryCloudPlatformQuery =
      "(SELECT pr.google_project_id "
          + "  FROM project_resource pr "
          + "  WHERE pr.id = dataset.project_resource_id) as google_project_id, "
          + "(SELECT sar.name "
          + "  FROM dataset_storage_account dsa "
          + "    LEFT JOIN storage_account_resource sar "
          + "      on dsa.storage_account_resource_id = sar.id "
          + "  WHERE dsa.dataset_id = dataset.id) as storage_account_name, ";

  private static final String datasetStorageQuery =
      "(SELECT jsonb_agg(json_build_object( "
          + "'region', region, "
          + "'cloudResource', cloud_resource, "
          + "'cloudPlatform', cloud_platform, "
          + "'datasetId', dataset_id)) "
          + "FROM storage_resource WHERE dataset_id = dataset.id) AS storage, ";

  private static final String billingProfileQuery =
      "(SELECT json_agg(json_build_object("
          + "'id', id, "
          + "'profileName', name, "
          + "'biller', biller, "
          + "'billingAccountId', billing_account_id, "
          + "'description', description, "
          + "'cloudPlatform', lower(cloud_platform), "
          + "'tenantId', tenant_id, "
          + "'subscriptionId', subscription_id, "
          + "'resourceGroupName', resource_group_name, "
          + "'applicationDeploymentName', application_deployment_name, "
          + "'createdDate', created_date, "
          + "'createdBy', created_by)) "
          + "FROM billing_profile where id = dataset.default_profile_id) AS billing_profiles ";

  @Autowired
  public DatasetDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      DatasetTableDao tableDao,
      DatasetRelationshipDao relationshipDao,
      AssetDao assetDao,
      ConfigurationService configurationService,
      ResourceService resourceService,
      StorageResourceDao storageResourceDao,
      JournalService journalService,
      @Qualifier("daoObjectMapper") ObjectMapper objectMapper)
      throws SQLException {
    this.jdbcTemplate = jdbcTemplate;
    this.tableDao = tableDao;
    this.relationshipDao = relationshipDao;
    this.assetDao = assetDao;
    this.configurationService = configurationService;
    this.resourceService = resourceService;
    this.storageResourceDao = storageResourceDao;
    this.journalService = journalService;
    this.objectMapper = objectMapper;
  }

  /**
   * Take an exclusive lock on the dataset object before doing something with it (e.g. delete). This
   * method returns successfully when this flight has an exclusive lock on the dataset object, and
   * throws an exception in all other cases. So, multiple locks can succeed with no errors. Logic
   * flow of the method: 1. Update the dataset record to give this flight the lock. 2. Throw an
   * exception if no records were updated.
   *
   * @param datasetId id of the dataset to lock, this is a unique column
   * @param flightId flight id that wants to lock the dataset
   * @throws DatasetLockException if the dataset is locked by another flight, either a shared or
   *     exclusive lock
   * @throws DatasetNotFoundException if the dataset does not exist
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void lockExclusive(UUID datasetId, String flightId) {
    if (flightId == null) {
      throw new DatasetLockException("Locking flight id cannot be null");
    }

    logger.debug(
        "Lock Operation: Adding exclusive lock for datasetId: {}, flightId: {}",
        datasetId,
        flightId);
    // update the dataset entry and lock it by setting the flight id
    String sql =
        "UPDATE dataset SET flightid = :flightid "
            + "WHERE id = :datasetid AND (flightid IS NULL OR flightid = :flightid) AND CARDINALITY(sharedlock) = 0";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("datasetid", datasetId).addValue("flightid", flightId);

    performLockQuery(sql, params, LockType.LockExclusive, datasetId);

    logger.debug(
        "Lock Operation: Exclusive lock acquired for dataset {}, flight {}", datasetId, flightId);
  }

  /**
   * Release the exclusive lock on the dataset object when finished doing something with it (e.g.
   * delete, create). If the dataset is not exclusively locked by this flight, then the method is a
   * no-op. It does not throw an exception in this case. So, multiple unlocks can succeed with no
   * errors. The method does return a boolean indicating whether any rows were updated or not. So,
   * callers can decide to throw an error if the unlock was a no-op.
   *
   * @param datasetId id of the dataset to unlock, this is a unique column
   * @param flightId flight id that wants to unlock the dataset
   * @return true if a dataset exclusive lock was released, false otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean unlockExclusive(UUID datasetId, String flightId) {
    logger.debug(
        "Lock Operation: Unlocking exclusive lock for datasetId: {}, flightId: {}",
        datasetId,
        flightId);
    // update the dataset entry to remove the flightid IF it is currently set to this flightid
    String sql =
        "UPDATE dataset SET flightid = NULL " + "WHERE id = :datasetid AND flightid = :flightid";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("datasetid", datasetId).addValue("flightid", flightId);

    int numRowsUpdated = performLockQuery(sql, params, LockType.UnlockExclusive, null);

    boolean unlockSucceeded = (numRowsUpdated == 1);
    logger.debug(
        "Lock Operation: Unlock exclusive successful? {}; for datasetId: {}, flightId: {}",
        unlockSucceeded,
        datasetId,
        flightId);
    return unlockSucceeded;
  }

  /**
   * Take a shared lock on the dataset object before doing something with it (e.g. file ingest, file
   * delete). This method returns successfully when this flight has a shared lock on the dataset
   * object, and throws an exception in all other cases. So, multiple locks can succeed with no
   * errors. Logic flow of the method: 1. Update the dataset record to give this flight a shared
   * lock. 2. Throw an exception if no records were updated.
   *
   * @param datasetId id of the dataset to lock, this is a unique column
   * @param flightId flight id that wants to lock the dataset
   * @throws DatasetLockException if another flight has an exclusive lock on the dataset
   * @throws DatasetNotFoundException if the dataset does not exist
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void lockShared(UUID datasetId, String flightId) {
    if (flightId == null) {
      throw new DatasetLockException("Locking flight id cannot be null");
    }
    logger.debug(
        "Lock Operation: Adding shared lock for datasetId: {}, flightId: {}", datasetId, flightId);
    // update the dataset entry and lock it by adding the flight id to the shared lock array column
    String sql =
        "UPDATE dataset SET sharedlock = "
            +
            // the SQL below appends flightid to an existing array
            // it does this by:
            //   1. append the flightid to the existing list: ARRAY_APPEND(sharedlock,
            // :flightid::text)
            //   2. convert the array to a list of rows: UNNEST
            //   3. select only the distinct rows, to make sure we don't have duplicates: DISTINCT
            // arrayelt
            //   4. convert the rows back to an array: ARRAY_AGG
            // the ARRAY_AGG function will return null if passed zero rows, but that will never be
            // the case here
            // because we are appending a new element, so there will always be at least one row
            "(SELECT ARRAY_AGG(DISTINCT arrayelt) "
            + "FROM UNNEST(ARRAY_APPEND(sharedlock, :flightid::text)) arrayelt) "
            + "WHERE id = :datasetid AND flightid IS NULL";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("datasetid", datasetId).addValue("flightid", flightId);

    int numRowsUpdated = performLockQuery(sql, params, LockType.LockShared, datasetId);

    logger.debug(
        "Lock Operation: Shared lock acquired for dataset {}, flight {}, with {} rows updated",
        datasetId,
        flightId,
        numRowsUpdated);
  }

  /**
   * Release the shared lock on the dataset object when finished doing something with it (e.g. file
   * ingest/delete). If this flight does not have a shared lock on the dataset object, then the
   * method is a no-op. It does not throw an exception in this case. So, multiple unlocks can
   * succeed with no errors. The method does return a boolean indicating whether any rows were
   * updated or not. So, callers can decide to throw an error if the unlock was a no-op.
   *
   * @param datasetId id of the dataset to unlock, this is a unique column
   * @param flightId flight id that wants to unlock the dataset
   * @return true if a dataset shared lock was released, false otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean unlockShared(UUID datasetId, String flightId) {
    logger.debug(
        "Lock Operation: Unlocking shared lock for datasetId: {}, flightId: {}",
        datasetId,
        flightId);
    // update the dataset entry to remove the flightid from the sharedlock list IF it is currently
    // included there
    String sql =
        "UPDATE dataset SET sharedlock = ARRAY_REMOVE(sharedlock, :flightid::text) "
            + "WHERE id = :datasetid AND :flightid = ANY(sharedlock)";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("datasetid", datasetId).addValue("flightid", flightId);
    logger.debug("Unlocking shared lock for datasetId: {}, flightId: {}", datasetId, flightId);

    int numRowsUpdated = performLockQuery(sql, params, LockType.UnlockShared, null);

    boolean unlockSucceeded = (numRowsUpdated == 1);
    logger.debug(
        "Lock Operation: Unlock shared successful? {}; for datasetId: {}, flightId: {}",
        unlockSucceeded,
        datasetId,
        flightId);
    return unlockSucceeded;
  }

  private enum LockType {
    LockExclusive,
    LockShared,
    UnlockExclusive,
    UnlockShared
  }

  private int performLockQuery(
      String sql, MapSqlParameterSource params, LockType lockType, UUID datasetId) {
    int numRowsUpdated = 0;
    DataAccessException faultToInsert = getFaultToInsert(lockType);
    try {
      if (faultToInsert != null) {
        logger.info("Test: Inserting fault to lock/unlock operation.");
        throw faultToInsert;
      }

      numRowsUpdated = jdbcTemplate.update(sql, params);

      if (numRowsUpdated == 0
          && (lockType.equals(LockType.LockExclusive) || lockType.equals(LockType.LockShared))) {
        // this method checks if the dataset exists
        // if it does not exist, then the method throws a DatasetNotFoundException
        // we don't need the result (dataset summary) here, just the existence check,
        // so ignore the return value.
        retrieveSummaryById(datasetId);

        throw new DatasetLockException("Failed to lock the dataset");
      }
    } catch (DatasetNotFoundException notFound) {
      logger.error(
          "Dataset lock failed: Dataset not found. Lock Type: {}, DatasetId: {}",
          lockType,
          datasetId);
      throw notFound;
    } catch (DatasetLockException lockException) {
      logger.error(
          "Dataset lock failed: Failed to lock dataset. Lock Type: {}, DatasetId: {}",
          lockType,
          datasetId);
      throw lockException;
    } catch (DataAccessException dataAccessException) {
      if (retryQuery(dataAccessException)) {
        logger.error(
            "Dataset lock failed with retryable exception. Lock Type: {}, DatasetId: {}",
            lockType,
            datasetId);
        throw new RetryQueryException("Retry", dataAccessException);
      }
      logger.error(
          "Dataset lock failed with fatal exception. Lock Type: {}, DatasetId: {}",
          lockType,
          datasetId);
      throw dataAccessException;
    }

    logger.debug("numRowsUpdated=" + numRowsUpdated);
    return numRowsUpdated;
  }

  private DataAccessException getFaultToInsert(LockType lockType) {
    // fault insert for tests DatasetConnectedTest & FileOperationTests
    ConfigEnum RetryableFault;
    ConfigEnum FatalFault;

    if (lockType.equals(LockType.LockExclusive) || lockType.equals(LockType.LockShared)) {
      RetryableFault = ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT;
      FatalFault = ConfigEnum.FILE_INGEST_LOCK_FATAL_FAULT;
    } else {
      RetryableFault = ConfigEnum.FILE_INGEST_UNLOCK_RETRY_FAULT;
      FatalFault = ConfigEnum.FILE_INGEST_UNLOCK_FATAL_FAULT;
    }

    if (configurationService.testInsertFault(RetryableFault)) {
      logger.info("{} - inserting RETRY fault to throw", lockType);
      return new OptimisticLockingFailureException(
          "TEST RETRY - RETRYABLE EXCEPTION - insert fault, throwing exception");
    } else if (configurationService.testInsertFault(FatalFault)) {
      logger.info("{} - insert FATAL fault to throw", lockType);
      return new DataIntegrityViolationException(
          "TEST RETRY - FATAL EXCEPTION - insert fault, throwing exception");
    }
    return null;
  }

  /**
   * Create a new dataset object and lock it. An exception is thrown if the dataset already exists.
   * The correct order to call the DatasetDao methods when creating a dataset is: createAndLock,
   * unlock.
   *
   * @param dataset the dataset object to create
   * @return the id of the new dataset
   * @throws IOException
   * @throws InvalidDatasetException if a row already exists with this dataset name
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createAndLock(Dataset dataset, String flightId, AuthenticatedUserRequest userReq)
      throws IOException {
    logger.debug(
        "Lock Operation: createAndLock datasetId: {} for flightId: {}", dataset.getId(), flightId);
    String sql =
        "INSERT INTO dataset "
            + "(name, default_profile_id, id, project_resource_id, application_resource_id, flightid, description, "
            + "secure_monitoring, phs_id, self_hosted, properties, sharedlock) "
            + "VALUES (:name, :default_profile_id, :id, :project_resource_id, :application_resource_id, :flightid, "
            + ":description, :secure_monitoring, :phs_id, :self_hosted, cast(:properties as jsonb), ARRAY[]::TEXT[]) ";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", dataset.getName())
            .addValue("default_profile_id", dataset.getDefaultProfileId())
            .addValue("project_resource_id", dataset.getProjectResourceId())
            .addValue("application_resource_id", dataset.getApplicationDeploymentResourceId())
            .addValue("id", dataset.getId())
            .addValue("flightid", flightId)
            .addValue("description", dataset.getDescription())
            .addValue("secure_monitoring", dataset.isSecureMonitoringEnabled())
            .addValue("phs_id", dataset.getPhsId())
            .addValue("self_hosted", dataset.isSelfHosted())
            .addValue(
                "properties", DaoUtils.propertiesToString(objectMapper, dataset.getProperties()));

    DaoKeyHolder keyHolder = new DaoKeyHolder();
    try {
      jdbcTemplate.update(sql, params, keyHolder);
    } catch (DuplicateKeyException dkEx) {
      throw new InvalidDatasetException(
          "Dataset name or id already exists: " + dataset.getName() + ", " + dataset.getId(), dkEx);
    }
    dataset.createdDate(keyHolder.getCreatedDate());
    tableDao.createTables(dataset.getId(), dataset.getTables());
    relationshipDao.createDatasetRelationships(dataset.getRelationships());
    assetDao.createAssets(dataset);
    storageResourceDao.createStorageAttributes(
        dataset.getDatasetSummary().getStorage(), dataset.getId());

    logger.debug("end of createAndLock datasetId: {} for flightId: {}", dataset.getId(), flightId);
  }

  /**
   * TESTING ONLY. This method returns the internal state of the exclusive lock on a dataset. It is
   * protected because it's for use in tests only. Currently, we don't expose the lock state of a
   * dataset outside of the DAO for other API code to consume.
   *
   * @param id the dataset id
   * @return the flightid that holds an exclusive lock. null if none.
   */
  protected String getExclusiveLock(UUID id) {
    try {
      String sql = "SELECT flightid FROM dataset WHERE id = :id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(sql, params, String.class);
    } catch (EmptyResultDataAccessException ex) {
      throw new DatasetNotFoundException("Dataset not found for id " + id);
    }
  }

  /**
   * TESTING ONLY. This method returns the internal state of the shared locks on a dataset. It is
   * protected because it's for use in tests only. Currently, we don't expose the lock state of a
   * dataset outside of the DAO for other API code to consume.
   *
   * @param id the dataset id
   * @return the array of flight ids that hold shared locks. empty if no shared locks are taken out.
   */
  protected String[] getSharedLocks(UUID id) {
    try {
      String sql = "SELECT sharedlock FROM dataset WHERE id = :id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      Array arr = jdbcTemplate.queryForObject(sql, params, Array.class);
      if (arr == null) {
        throw new CorruptMetadataException(
            "Dataset shared locks array column should not be null. id " + id);
      }
      return (String[]) arr.getArray();
    } catch (EmptyResultDataAccessException | SQLException ex) {
      throw new DatasetNotFoundException("Dataset not found for id " + id);
    }
  }

  @Transactional
  public boolean delete(UUID id, AuthenticatedUserRequest userReq) {
    int rowsAffected =
        jdbcTemplate.update(
            "DELETE FROM dataset WHERE id = :id", new MapSqlParameterSource().addValue("id", id));
    return rowsAffected > 0;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean deleteByNameAndFlight(String datasetName, String flightId) {
    String sql = "DELETE FROM dataset WHERE name = :name AND flightid = :flightid";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("name", datasetName).addValue("flightid", flightId);
    int rowsAffected = jdbcTemplate.update(sql, params);

    // TODO: shouldn't we also be deleting from the auxiliary daos for this dataset (table,
    // relationship, asset)?

    return rowsAffected > 0;
  }

  public Dataset retrieve(UUID id) {
    DatasetSummary summary = retrieveSummaryById(id);
    return retrieveWorker(summary);
  }

  /**
   * This is a convenience wrapper that returns a dataset only if it is NOT exclusively locked. This
   * method is intended for user-facing API calls (e.g. from RepositoryApiController).
   *
   * @param id the dataset id
   * @return the DatasetSummary object
   */
  public Dataset retrieveAvailable(UUID id) {
    DatasetSummary summary = retrieveSummaryById(id, true);
    return retrieveWorker(summary);
  }

  public Dataset retrieveByName(String name) {
    DatasetSummary summary = retrieveSummaryByName(name);
    return retrieveWorker(summary);
  }

  private Dataset retrieveWorker(DatasetSummary summary) {
    Dataset dataset = null;
    try {
      if (summary != null) {
        summary.storage(storageResourceDao.getStorageResourcesByDatasetId(summary.getId()));
        dataset = new Dataset(summary);
        dataset.tables(tableDao.retrieveTables(dataset.getId()));
        relationshipDao.retrieve(dataset);
        assetDao.retrieve(dataset);
        // Retrieve the project and application deployment resource associated with the dataset
        // This is a bit sketchy filling in the object via a dao in another package.
        // It seemed like the cleanest thing to me at the time.
        if (dataset.getProjectResourceId() != null) {
          dataset.projectResource(
              resourceService.getProjectResource(dataset.getProjectResourceId()));
        }
        if (dataset.getApplicationDeploymentResourceId() != null) {
          dataset.applicationDeploymentResource(
              resourceService.getApplicationDeploymentResource(
                  dataset.getApplicationDeploymentResourceId()));
        }
      }
      return dataset;
    } catch (EmptyResultDataAccessException ex) {
      throw new CorruptMetadataException("Inconsistent data", ex);
    }
  }

  /**
   * This is a convenience wrapper that returns a dataset, regardless of whether it is exclusively
   * locked. Most places in the API code that are retrieving a dataset will call this method.
   *
   * @param id the dataset id
   * @return the DatasetSummary object
   */
  public DatasetSummary retrieveSummaryById(UUID id) {
    return retrieveSummaryById(id, false);
  }

  /**
   * Retrieves a DatasetSummary object from the dataset id.
   *
   * @param id the dataset id
   * @param onlyRetrieveAvailable true to exclude datasets that are exclusively locked, false to
   *     include all datasets
   * @return the DatasetSummary object
   */
  public DatasetSummary retrieveSummaryById(UUID id, boolean onlyRetrieveAvailable) {
    try {
      String sql =
          "SELECT "
              + summaryQueryColumns
              + summaryCloudPlatformQuery
              + datasetStorageQuery
              + billingProfileQuery
              + "FROM dataset "
              + "WHERE dataset.id = :id";
      if (onlyRetrieveAvailable) { // exclude datasets that are exclusively locked
        sql += " AND dataset.flightid IS NULL";
      }
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new DatasetNotFoundException("Dataset not found for id " + id.toString());
    }
  }

  public DatasetSummary retrieveSummaryByName(String name) {
    try {
      String sql =
          "SELECT "
              + summaryQueryColumns
              + summaryCloudPlatformQuery
              + datasetStorageQuery
              + billingProfileQuery
              + "FROM dataset "
              + "WHERE dataset.name = :name";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
      return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new DatasetNotFoundException("Dataset not found for name " + name);
    }
  }

  /**
   * Fetch a list of all the available datasets. This method returns summary objects, which do not
   * include sub-objects associated with datasets (e.g. tables). Note that this method will only
   * return datasets that are NOT exclusively locked.
   *
   * @param offset skip this many datasets from the beginning of the list (intended for "scrolling"
   *     behavior)
   * @param limit only return this many datasets in the list
   * @param sort field for order by clause. possible values are: name, description, created_date
   * @param direction asc or desc
   * @param filter string to match (SQL ILIKE) in dataset name or description
   * @param accessibleDatasetIds list of dataset ids that caller has access to (fetched from IAM
   *     service)
   * @return a list of dataset summary objects
   */
  @Transactional(
      readOnly = true,
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE)
  public MetadataEnumeration<DatasetSummary> enumerate(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      Collection<UUID> accessibleDatasetIds) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    List<String> whereClauses = new ArrayList<>();
    DaoUtils.addAuthzIdsClause(accessibleDatasetIds, params, whereClauses, TABLE_NAME);
    whereClauses.add(" dataset.flightid IS NULL"); // exclude datasets that are exclusively locked

    // get total count of objects
    String countSql =
        "SELECT count(id) AS total FROM dataset WHERE " + StringUtils.join(whereClauses, " AND ");
    Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);
    if (total == null) {
      throw new CorruptMetadataException("Impossible null value from total count");
    }

    // add the filters to the clause to get the actual items
    DaoUtils.addFilterClause(filter, params, whereClauses, TABLE_NAME);
    DaoUtils.addRegionFilterClause(region, params, whereClauses, "dataset.id");

    String whereSql = "";
    if (!whereClauses.isEmpty()) {
      whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
    }

    // get the filtered total count of objects without offset and limit
    String filteredTotalSql =
        "SELECT count(id) AS total FROM dataset WHERE " + StringUtils.join(whereClauses, " AND ");

    Integer filteredTotal = jdbcTemplate.queryForObject(filteredTotalSql, params, Integer.class);
    if (filteredTotal == null) {
      throw new CorruptMetadataException("Impossible null value from filtered count");
    }

    String sql =
        "SELECT "
            + summaryQueryColumns
            + summaryCloudPlatformQuery
            + datasetStorageQuery
            + billingProfileQuery
            + "FROM dataset "
            + whereSql
            + DaoUtils.orderByClause(sort, direction, TABLE_NAME)
            + " OFFSET :offset LIMIT :limit";
    params.addValue("offset", offset).addValue("limit", limit);
    List<DatasetSummary> summaries = jdbcTemplate.query(sql, params, new DatasetSummaryMapper());

    return new MetadataEnumeration<DatasetSummary>()
        .items(summaries)
        .total(total)
        .filteredTotal(filteredTotal);
  }

  private class DatasetSummaryMapper implements RowMapper<DatasetSummary> {
    public DatasetSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
      UUID datasetId = rs.getObject("id", UUID.class);
      List<? extends StorageResource<?, ?>> storageResources;
      try {
        storageResources =
            objectMapper.readValue(rs.getString("storage"), new TypeReference<>() {});
      } catch (JsonProcessingException e) {
        throw new CorruptMetadataException(
            String.format("Invalid storage for dataset - id: %s", datasetId), e);
      }
      final List<BillingProfileModel> billingProfileModels;
      try {
        billingProfileModels =
            objectMapper.readValue(rs.getString("billing_profiles"), new TypeReference<>() {});
      } catch (JsonProcessingException e) {
        throw new CorruptMetadataException(
            String.format("Invalid billing profiles for dataset - id: %s", datasetId), e);
      }
      Object properties = null;
      String rsProperties = rs.getString("properties");
      if (rsProperties != null) {
        try {
          properties = objectMapper.readValue(rsProperties, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
          throw new CorruptMetadataException(
              String.format("Invalid properties field for dataset - id: %s", datasetId), e);
        }
      }

      boolean isAzure =
          storageResources.stream().anyMatch(sr -> sr.getCloudPlatform() == CloudPlatform.AZURE);

      CloudPlatform datasetCloudPlatform = isAzure ? CloudPlatform.AZURE : CloudPlatform.GCP;

      return new DatasetSummary()
          .id(datasetId)
          .name(rs.getString("name"))
          .description(rs.getString("description"))
          .defaultProfileId(rs.getObject("default_profile_id", UUID.class))
          .projectResourceId(rs.getObject("project_resource_id", UUID.class))
          .applicationDeploymentResourceId(rs.getObject("application_resource_id", UUID.class))
          .createdDate(rs.getTimestamp("created_date").toInstant())
          .storage(storageResources)
          .billingProfiles(billingProfileModels)
          .secureMonitoringEnabled(rs.getBoolean("secure_monitoring"))
          .cloudPlatform(datasetCloudPlatform)
          .dataProject(rs.getString("google_project_id"))
          .storageAccount(rs.getString("storage_account_name"))
          .phsId(rs.getString("phs_id"))
          .selfHosted(rs.getBoolean("self_hosted"))
          .properties(properties);
    }
  }

  /**
   * Update a dataset according to specified fields in the patch request. Any fields unspecified in
   * the request will remain unaltered.
   *
   * @param id dataset UUID
   * @param patchRequest updates to merge with existing dataset
   * @return whether the dataset record was updated
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean patch(
      UUID id, DatasetPatchRequestModel patchRequest, AuthenticatedUserRequest userReq) {
    String sql =
        "UPDATE dataset SET phs_id = COALESCE(:phs_id, phs_id), "
            + "description = COALESCE(:description, description), "
            + "properties = COALESCE(cast(:properties as jsonb), properties) WHERE id = :id";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("phs_id", patchRequest.getPhsId())
            .addValue("description", patchRequest.getDescription())
            .addValue(
                "properties",
                DaoUtils.propertiesToString(objectMapper, patchRequest.getProperties()))
            .addValue("id", id);

    int rowsAffected = jdbcTemplate.update(sql, params);
    boolean patchSucceeded = (rowsAffected == 1);

    if (patchSucceeded) {
      logger.info("Dataset {} patched with {}", id, patchRequest.toString());
      journalService.journalUpdate(
          userReq, id, IamResourceType.DATASET, "Patched dataset.", params.getValues());
    }
    return patchSucceeded;
  }

  /**
   * Probe to see if can access database
   *
   * @return status and if failure, exception message in RepositoryStatusModelSystems model
   */
  public RepositoryStatusModelSystems statusCheck() {
    String sql = "SELECT count(1)";
    MapSqlParameterSource params = new MapSqlParameterSource();
    jdbcTemplate.queryForObject(sql, params, Integer.class);
    try {
      jdbcTemplate.queryForObject(sql, params, Integer.class);
      return new RepositoryStatusModelSystems().ok(true);
    } catch (Exception ex) {
      String errorMsg = "Database status check failed";
      logger.error(errorMsg, ex);
      return new RepositoryStatusModelSystems().ok(false).message(errorMsg + ": " + ex);
    }
  }
}
