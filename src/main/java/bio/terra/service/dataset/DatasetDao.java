package bio.terra.service.dataset;

import bio.terra.common.DaoUtils;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static bio.terra.common.DaoUtils.retryQuery;

@Repository
public class DatasetDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DatasetTableDao tableDao;
    private final DatasetRelationshipDao relationshipDao;
    private final AssetDao assetDao;
    private final ConfigurationService configurationService;

    private static final Logger logger = LoggerFactory.getLogger(DatasetDao.class);

    @Autowired
    public DatasetDao(NamedParameterJdbcTemplate jdbcTemplate,
                      DatasetTableDao tableDao,
                      DatasetRelationshipDao relationshipDao,
                      AssetDao assetDao,
                      ConfigurationService configurationService) throws SQLException {
        this.jdbcTemplate = jdbcTemplate;
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
        this.configurationService = configurationService;
    }

    /**
     * Take an exclusive lock on the dataset object before doing something with it (e.g. delete).
     * This method returns successfully when this flight has an exclusive lock on the dataset object, and
     * throws an exception in all other cases. So, multiple locks can succeed with no errors. Logic flow of the method:
     * 1. Update the dataset record to give this flight the lock.
     * 2. Throw an exception if no records were updated.
     *
     * @param datasetId id of the dataset to lock, this is a unique column
     * @param flightId  flight id that wants to lock the dataset
     * @throws DatasetLockException     if the dataset is locked by another flight, either a shared or exclusive lock
     * @throws DatasetNotFoundException if the dataset does not exist
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void lockExclusive(UUID datasetId, String flightId) {
        if (flightId == null) {
            throw new DatasetLockException("Locking flight id cannot be null");
        }

        logger.debug("Lock Operation: Adding exclusive lock for datasetId: {}, flightId: {}", datasetId, flightId);
        // update the dataset entry and lock it by setting the flight id
        String sql = "UPDATE dataset SET flightid = :flightid " +
            "WHERE id = :datasetid AND (flightid IS NULL OR flightid = :flightid) AND CARDINALITY(sharedlock) = 0";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);


        performLockQuery(sql, params, LockType.LockExclusive, datasetId);

        logger.debug("Lock Operation: Exclusive lock acquired for dataset {}, flight {}", datasetId, flightId);
    }

    /**
     * Release the exclusive lock on the dataset object when finished doing something with it (e.g. delete, create).
     * If the dataset is not exclusively locked by this flight, then the method is a no-op. It does not throw an
     * exception in this case. So, multiple unlocks can succeed with no errors. The method does return a boolean
     * indicating whether any rows were updated or not. So, callers can decide to throw an error if the unlock
     * was a no-op.
     *
     * @param datasetId id of the dataset to unlock, this is a unique column
     * @param flightId  flight id that wants to unlock the dataset
     * @return true if a dataset exclusive lock was released, false otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean unlockExclusive(UUID datasetId, String flightId) {
        logger.debug("Lock Operation: Unlocking exclusive lock for datasetId: {}, flightId: {}", datasetId, flightId);
        // update the dataset entry to remove the flightid IF it is currently set to this flightid
        String sql = "UPDATE dataset SET flightid = NULL " +
            "WHERE id = :datasetid AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);

        int numRowsUpdated = performLockQuery(sql, params, LockType.UnlockExclusive, null);

        boolean unlockSucceeded = (numRowsUpdated == 1);
        logger.debug("Lock Operation: Unlock exclusive successful? {}; for datasetId: {}, flightId: {}",
            unlockSucceeded, datasetId, flightId);
        return unlockSucceeded;
    }

    /**
     * Take a shared lock on the dataset object before doing something with it (e.g. file ingest, file delete).
     * This method returns successfully when this flight has a shared lock on the dataset object, and
     * throws an exception in all other cases. So, multiple locks can succeed with no errors. Logic flow of the method:
     * 1. Update the dataset record to give this flight a shared lock.
     * 2. Throw an exception if no records were updated.
     *
     * @param datasetId id of the dataset to lock, this is a unique column
     * @param flightId  flight id that wants to lock the dataset
     * @throws DatasetLockException     if another flight has an exclusive lock on the dataset
     * @throws DatasetNotFoundException if the dataset does not exist
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void lockShared(UUID datasetId, String flightId) {
        if (flightId == null) {
            throw new DatasetLockException("Locking flight id cannot be null");
        }
        logger.debug("Lock Operation: Adding shared lock for datasetId: {}, flightId: {}", datasetId, flightId);
        // update the dataset entry and lock it by adding the flight id to the shared lock array column
        String sql = "UPDATE dataset SET sharedlock = " +
            // the SQL below appends flightid to an existing array
            // it does this by:
            //   1. append the flightid to the existing list: ARRAY_APPEND(sharedlock, :flightid::text)
            //   2. convert the array to a list of rows: UNNEST
            //   3. select only the distinct rows, to make sure we don't have duplicates: DISTINCT arrayelt
            //   4. convert the rows back to an array: ARRAY_AGG
            // the ARRAY_AGG function will return null if passed zero rows, but that will never be the case here
            // because we are appending a new element, so there will always be at least one row
            "(SELECT ARRAY_AGG(DISTINCT arrayelt) " +
            "FROM UNNEST(ARRAY_APPEND(sharedlock, :flightid::text)) arrayelt) " +

            "WHERE id = :datasetid AND flightid IS NULL";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);


        int numRowsUpdated = performLockQuery(sql, params, LockType.LockShared, datasetId);

        logger.debug("Lock Operation: Shared lock acquired for dataset {}, flight {}, with {} rows updated",
            datasetId, flightId, numRowsUpdated);
    }

    /**
     * Release the shared lock on the dataset object when finished doing something with it (e.g. file ingest/delete).
     * If this flight does not have a shared lock on the dataset object, then the method is a no-op. It does not throw
     * an exception in this case. So, multiple unlocks can succeed with no errors. The method does return a boolean
     * indicating whether any rows were updated or not. So, callers can decide to throw an error if the unlock was
     * a no-op.
     *
     * @param datasetId id of the dataset to unlock, this is a unique column
     * @param flightId  flight id that wants to unlock the dataset
     * @return true if a dataset shared lock was released, false otherwise
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean unlockShared(UUID datasetId, String flightId) {
        logger.debug("Lock Operation: Unlocking shared lock for datasetId: {}, flightId: {}", datasetId, flightId);
        // update the dataset entry to remove the flightid from the sharedlock list IF it is currently included there
        String sql = "UPDATE dataset SET sharedlock = ARRAY_REMOVE(sharedlock, :flightid::text) " +
            "WHERE id = :datasetid AND :flightid = ANY(sharedlock)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);
        logger.debug("Unlocking shared lock for datasetId: {}, flightId: {}", datasetId, flightId);

        int numRowsUpdated = performLockQuery(sql, params, LockType.UnlockShared, null);

        boolean unlockSucceeded = (numRowsUpdated == 1);
        logger.debug("Lock Operation: Unlock shared successful? {}; for datasetId: {}, flightId: {}",
            unlockSucceeded, datasetId, flightId);
        return unlockSucceeded;
    }

    private enum LockType {
        LockExclusive,
        LockShared,
        UnlockExclusive,
        UnlockShared
    }

    private int performLockQuery(String sql, MapSqlParameterSource params,
                                 LockType lockType, UUID datasetId) {
        int numRowsUpdated = 0;
        DataAccessException faultToInsert = getFaultToInsert(lockType);
        try {
            if (faultToInsert != null) {
                logger.info("Test: Inserting fault to lock/unlock operation.");
                throw faultToInsert;
            }

            numRowsUpdated = jdbcTemplate.update(sql, params);

            if (numRowsUpdated == 0 &&
                (lockType.equals(LockType.LockExclusive) || lockType.equals(LockType.LockShared))) {
                // this method checks if the dataset exists
                // if it does not exist, then the method throws a DatasetNotFoundException
                // we don't need the result (dataset summary) here, just the existence check,
                // so ignore the return value.
                retrieveSummaryById(datasetId);

                throw new DatasetLockException("Failed to lock the dataset");
            }
        } catch (DatasetNotFoundException notFound) {
            logger.error("Dataset lock failed: Dataset not found. Lock Type: {}, DatasetId: {}", lockType, datasetId);
            throw notFound;
        } catch (DatasetLockException lockException) {
            logger.error("Dataset lock failed: Failed to lock dataset. Lock Type: {}, DatasetId: {}",
                lockType, datasetId);
            throw lockException;
        } catch (DataAccessException dataAccessException) {
            if (retryQuery(dataAccessException)) {
                logger.error("Dataset lock failed with retryable exception. Lock Type: {}, DatasetId: {}",
                    lockType, datasetId);
                throw new RetryQueryException("Retry", dataAccessException);
            }
            logger.error("Dataset lock failed with fatal exception. Lock Type: {}, DatasetId: {}",
                lockType, datasetId);
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
     * The correct order to call the DatasetDao methods when creating a dataset is: createAndLock, unlock.
     *
     * @param dataset the dataset object to create
     * @return the id of the new dataset
     * @throws SQLException
     * @throws IOException
     * @throws InvalidDatasetException if a row already exists with this dataset name
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public UUID createAndLock(Dataset dataset, String flightId) throws IOException, SQLException {
        logger.debug("Lock Operation: createAndLock datasetId: {} for flightId: {}", dataset.getId(), flightId);
        // TODO why did this method think it already had a dataset id? ^
        Timestamp created_date = new Timestamp(dataset.getCreatedDate().toEpochMilli());
        String sql = "INSERT INTO dataset " +
            "(name, default_profile_id, id, created_date, flightid, description, sharedlock) " +
            "VALUES (:name, :default_profile_id, :id, :created_date, :flightid, :description, ARRAY[]::TEXT[]) ";

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", dataset.getName())
            .addValue("default_profile_id", dataset.getDefaultProfileId())
            .addValue("id", dataset.getId())
            .addValue("created_date", created_date)
            .addValue("flightid", flightId)
            .addValue("description", dataset.getDescription());
        try {
            jdbcTemplate.update(sql, params);
        } catch (DuplicateKeyException dkEx) {
            throw new InvalidDatasetException("Dataset name already exists: " + dataset.getName(), dkEx);
        }

        tableDao.createTables(dataset.getId(), dataset.getTables());
        relationshipDao.createDatasetRelationships(dataset);
        assetDao.createAssets(dataset);

        logger.debug("end of createAndLock datasetId: {} for flightId: {}", dataset.getId(), flightId);
        return dataset.getId();
    }

    /**
     * TESTING ONLY. This method returns the internal state of the exclusive lock on a dataset.
     * It is protected because it's for use in tests only.
     * Currently, we don't expose the lock state of a dataset outside of the DAO for other API code to consume.
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
     * TESTING ONLY. This method returns the internal state of the shared locks on a dataset.
     * It is protected because it's for use in tests only.
     * Currently, we don't expose the lock state of a dataset outside of the DAO for other API code to consume.
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
                throw new CorruptMetadataException("Dataset shared locks array column should not be null. id " + id);
            }
            return (String[]) arr.getArray();
        } catch (EmptyResultDataAccessException | SQLException ex) {
            throw new DatasetNotFoundException("Dataset not found for id " + id);
        }
    }

    @Transactional
    public boolean delete(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE id = :id",
            new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean deleteByNameAndFlight(String datasetName, String flightId) {
        String sql = "DELETE FROM dataset WHERE name = :name AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", datasetName)
            .addValue("flightid", flightId);
        int rowsAffected = jdbcTemplate.update(sql, params);

        // TODO: shouldn't we also be deleting from the auxiliary daos for this dataset (table, relationship, asset)?

        return rowsAffected > 0;
    }

    public Dataset retrieve(UUID id) {
        DatasetSummary summary = retrieveSummaryById(id);
        return retrieveWorker(summary);
    }

    /**
     * This is a convenience wrapper that returns a dataset only if it is NOT exclusively locked.
     * This method is intended for user-facing API calls (e.g. from RepositoryApiController).
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
                dataset = new Dataset(summary);
                dataset.tables(tableDao.retrieveTables(dataset.getId()));
                relationshipDao.retrieve(dataset);
                assetDao.retrieve(dataset);
            }
            return dataset;
        } catch (EmptyResultDataAccessException ex) {
            throw new CorruptMetadataException("Inconsistent data", ex);
        }
    }

    /**
     * This is a convenience wrapper that returns a dataset, regardless of whether it is exclusively locked.
     * Most places in the API code that are retrieving a dataset will call this method.
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
     * @param id                    the dataset id
     * @param onlyRetrieveAvailable true to exclude datasets that are exclusively locked, false to include all datasets
     * @return the DatasetSummary object
     */
    public DatasetSummary retrieveSummaryById(UUID id, boolean onlyRetrieveAvailable) {
        try {
            String sql = "SELECT " +
                "id, name, description, default_profile_id, created_date " +
                "FROM dataset WHERE id = :id";
            if (onlyRetrieveAvailable) { // exclude datasets that are exclusively locked
                sql += " AND flightid IS NULL";
            }
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for id " + id.toString());
        }
    }

    public DatasetSummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT " +
                "id, name, description, default_profile_id, created_date " +
                "FROM dataset WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for name " + name);
        }
    }

    /**
     * Fetch a list of all the available datasets.
     * This method returns summary objects, which do not include sub-objects associated with datasets (e.g. tables).
     * Note that this method will only return datasets that are NOT exclusively locked.
     *
     * @param offset               skip this many datasets from the beginning of the list
     *                             (intended for "scrolling" behavior)
     * @param limit                only return this many datasets in the list
     * @param sort                 field for order by clause. possible values are: name, description, created_date
     * @param direction            asc or desc
     * @param filter               string to match (SQL ILIKE) in dataset name or description
     * @param accessibleDatasetIds list of dataset ids that caller has access to (fetched from IAM service)
     * @return a list of dataset summary objects
     */
    public MetadataEnumeration<DatasetSummary> enumerate(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> accessibleDatasetIds
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        DaoUtils.addAuthzIdsClause(accessibleDatasetIds, params, whereClauses);
        whereClauses.add(" flightid IS NULL"); // exclude datasets that are exclusively locked

        // get total count of objects
        String countSql = "SELECT count(id) AS total FROM dataset WHERE " +
            StringUtils.join(whereClauses, " AND ");
        Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);

        // add the filter to the clause to get the actual items
        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }
        String sql = "SELECT " +
            "id, name, description, default_profile_id, created_date " +
            "FROM dataset " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<DatasetSummary> summaries = jdbcTemplate.query(sql, params, new DatasetSummaryMapper());

        return new MetadataEnumeration<DatasetSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    private static class DatasetSummaryMapper implements RowMapper<DatasetSummary> {
        public DatasetSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DatasetSummary()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .defaultProfileId(rs.getObject("default_profile_id", UUID.class))
                .createdDate(rs.getTimestamp("created_date").toInstant());
        }
    }

    /**
     * Probe to see if can access database
     */
    public boolean statusCheck() {
        String sql = "SELECT count(1)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        try {
            jdbcTemplate.queryForObject(sql, params, Integer.class);
            return true;
        } catch (Exception ex) {
            logger.error("Database status check failed: " + ex.getMessage());
            return false;
        }

    }
}
