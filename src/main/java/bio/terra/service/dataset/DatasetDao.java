package bio.terra.service.dataset;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoUtils;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.job.LockBehaviorFlags;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.common.MetadataEnumeration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class DatasetDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Connection connection;
    private final DatasetTableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    private static Logger logger = LoggerFactory.getLogger(DatasetDao.class);

    @Autowired
    public DatasetDao(DataRepoJdbcConfiguration jdbcConfiguration,
                    DatasetTableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao) throws SQLException {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
        connection = jdbcConfiguration.getDataSource().getConnection();
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }

    /**
     * Lock the dataset object before doing something with it (e.g. create, delete, bulk file load).
     * This method returns successfully when there is a dataset object locked by this flight, and throws an exception
     * in all other cases. Below is an outline of the logic flow of this method.
     *     1. Check if the dataset is already locked.
     *         a. If it's locked by THIS flight, then this must be a flight recovery, so return without error.
     *         b. If it's locked by ANOTHER flight, then throw an exception.
     *     2. Depending on the LockBehaviorFlag specified, throw an exception if the dataset does/not already exist.
     *     3. Either create a new dataset or update the existing one, and give this flight the lock.
     * @param datasetName name of the dataset to lock, this is a unique column
     * @param defaultProfileId default profile id of the dataset, this is a required column for creating a new dataset
     * @param flightId flight id that wants to lock the dataset
     * @param lockFlag flag specifying the lock behavior (i.e. whether to throw an exception if dataset does/not exist)
     * @throws DatasetLockException if the dataset is locked by another flight,
     *                              or if it does/not exist according to the LockBehaviorFlag specified
     */
    @Transactional(propagation =  Propagation.REQUIRED)
    public void lock(String datasetName, UUID defaultProfileId, String flightId, LockBehaviorFlags lockFlag) {
        if (flightId == null) {
            throw new DatasetLockException("Locking flight id cannot be null");
        }

        // lookup the dataset
        String sql = "SELECT flightid FROM dataset WHERE name = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", datasetName);
        List<String> selectResult = jdbcTemplate.query(sql, params,
            (rs, rowNum) -> rs.getString(1));

        boolean datasetExists = (selectResult.size() == 1);
        String foundFlightId = datasetExists ? selectResult.get(0) : null;
        boolean datasetLocked = foundFlightId != null;

        // if the dataset is already locked...
        if (datasetLocked && flightId.equals(foundFlightId)) {
            // ...by THIS flight, then this must be a flight recovery
            logger.debug("dataset already locked by THIS flight, must be a flight recovery");
            return; // return without error
        } else if (datasetLocked && !flightId.equals(foundFlightId)) {
            // ...by ANOTHER flight, then throw an exception
            logger.debug("dataset already locked by ANOTHER flight, error");
            throw new DatasetLockException("Dataset is locked by another flight: " + foundFlightId);
        }

        // at this point, we know that the dataset is not locked by anyone
        logger.debug("dataset not locked by anyone");

        // check the lock behavior flag and...
        if (lockFlag.equals(LockBehaviorFlags.LOCK_ONLY_IF_OBJECT_DOES_NOT_EXIST) && datasetExists) {
            // ...if we only want to lock a new object, throw an exception if one already exists
            throw new DatasetLockException("Dataset already exists. Not locking existing record.");
        } else if (lockFlag.equals(LockBehaviorFlags.LOCK_ONLY_IF_OBJECT_EXISTS) && !datasetExists) {
            // ...if we only want to lock an existing object, throw an exception if one doesn't exist
            throw new DatasetLockException("Dataset does not exist. Not creating new record to lock.");
        }

        // either create or update the dataset entry and lock it by setting the flight id
        sql = "INSERT INTO dataset (name, default_profile_id, flightid) " +
            "VALUES (:name, :default_profile_id, :flightid) " +
            "ON CONFLICT (name) " +
            "DO UPDATE SET flightid = :flightid";
        params = new MapSqlParameterSource()
            .addValue("name", datasetName)
            .addValue("default_profile_id", defaultProfileId)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        if (numRowsUpdated != 1) {
            logger.debug("numRowsUpdated=" + numRowsUpdated);
            throw new DatasetLockException("Failed to lock the dataset");
        }

        // at this point, we know the dataset exists and is locked by the current flight
        logger.debug("dataset locked by current flight");

        return;
    }

    /**
     * Unlock the dataset object when finished doing something with it (e.g. create, delete, bulk file load).
     * If the dataset is not locked by this flight, then the method is a no-op. It does not throw an exception in this
     * case. So, multiple unlocks can succeed with no errors. The method does return a boolean indicating whether any
     * rows were updated or not. So, callers can decide to throw an error if the unlock was a no-op.
     * @param datasetName name of the dataset to unlock, this is a unique column
     * @param flightId flight id that wants to unlock the dataset
     * @return true if a dataset was unlocked, false otherwise
     */
    @Transactional(propagation =  Propagation.REQUIRED)
    public boolean unlock(String datasetName, String flightId) {
        // update the dataset entry to remove the flightid IF it is currently set to this flightid
        String sql = "UPDATE dataset SET flightid = NULL " +
            "WHERE name = :name AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", datasetName)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        logger.debug("numRowsUpdated=" + numRowsUpdated);
        return (numRowsUpdated == 1);
    }

    /**
     * Create a new dataset object. The dataset must already be locked or an exception is thrown.
     * The correct order to call the DatasetDao methods when creating a dataset is: lock, create, unlock.
     * @param dataset the dataset object to create
     * @return the id of the new dataset
     * @throws SQLException
     * @throws DatasetLockException if the dataset object has not been locked before calling this method
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Dataset dataset) throws SQLException {
        String sql = "UPDATE dataset " +
            "SET description = :description, additional_profile_ids = :additional_profile_ids " +
            "WHERE name = :name";
        Array additionalProfileIds = DaoUtils.createSqlUUIDArray(connection, dataset.getAdditionalProfileIds());
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", dataset.getName())
            .addValue("description", dataset.getDescription())
            .addValue("additional_profile_ids", additionalProfileIds);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        if (numRowsUpdated == 0) {
            throw new DatasetLockException("Dataset must be locked before creating it.");
        }

        sql = "SELECT id, created_date FROM dataset WHERE name = :name";
        params = new MapSqlParameterSource().addValue("name", dataset.getName());
        List<Map<String, Object>> selectResult = jdbcTemplate.query(sql, params,
            (rs, rowNum) -> {
                Map<String, Object> rowMap = new HashMap<>();
                rowMap.put("id", rs.getObject("id", UUID.class));
                rowMap.put("created_date", rs.getTimestamp("created_date").toInstant());
                return rowMap;
            });
        dataset.id((UUID)selectResult.get(0).get("id"));
        dataset.createdDate((Instant)selectResult.get(0).get("created_date"));

        tableDao.createTables(dataset.getId(), dataset.getTables());
        relationshipDao.createDatasetRelationships(dataset);
        assetDao.createAssets(dataset);
        return dataset.getId();
    }

    @Transactional
    public boolean delete(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    @Transactional
    public boolean deleteByName(String datasetName) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE name = :name",
                new MapSqlParameterSource().addValue("name", datasetName));
        return rowsAffected > 0;
    }

    public Dataset retrieve(UUID id) {
        DatasetSummary summary = retrieveSummaryById(id);
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

    public DatasetSummary retrieveSummaryById(UUID id) {
        try {
            String sql = "SELECT id, name, description, default_profile_id, additional_profile_ids, created_date " +
                " FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for id " + id.toString());
        }
    }

    public DatasetSummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT id, name, description, default_profile_id, additional_profile_ids, created_date " +
                " FROM dataset WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for name " + name);
        }
    }

    // does not return sub-objects with datasets
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
        String sql = "SELECT id, name, description, created_date, default_profile_id, additional_profile_ids " +
            " FROM dataset " + whereSql +
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
                    .additionalProfileIds(DaoUtils.getUUIDList(rs, "additional_profile_ids"))
                    .createdDate(rs.getTimestamp("created_date").toInstant());
        }
    }
}
