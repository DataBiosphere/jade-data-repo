package bio.terra.service.dataset;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.common.MetadataEnumeration;
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

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
     * Lock the dataset object before doing something with it (e.g. delete, bulk file load).
     * This method returns successfully when there is a dataset object locked by this flight, and throws an exception
     * in all other cases. So, multiple locks can succeed with no errors. Logic flow of the method:
     *     1. Update the dataset record to give this flight the lock.
     *     2. Throw an exception if no records were updated.
     * @param datasetId id of the dataset to lock, this is a unique column
     * @param flightId flight id that wants to lock the dataset
     * @throws DatasetLockException if the dataset is locked by another flight or does not exist
     */
    @Transactional(propagation =  Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void lock(UUID datasetId, String flightId) {
        if (flightId == null) {
            throw new DatasetLockException("Locking flight id cannot be null");
        }

        // update the dataset entry and lock it by setting the flight id
        String sql = "UPDATE dataset SET flightid = :flightid " +
            "WHERE id = :datasetid AND (flightid IS NULL OR flightid = :flightid)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);

        // if no rows were updated, then throw an exception
        if (numRowsUpdated == 0) {
            logger.debug("numRowsUpdated=" + numRowsUpdated);
            throw new DatasetLockException("Failed to lock the dataset");
        }
    }

    /**
     * Unlock the dataset object when finished doing something with it (e.g. delete, bulk file load).
     * If the dataset is not locked by this flight, then the method is a no-op. It does not throw an exception in this
     * case. So, multiple unlocks can succeed with no errors. The method does return a boolean indicating whether any
     * rows were updated or not. So, callers can decide to throw an error if the unlock was a no-op.
     * @param datasetId id of the dataset to unlock, this is a unique column
     * @param flightId flight id that wants to unlock the dataset
     * @return true if a dataset was unlocked, false otherwise
     */
    @Transactional(propagation =  Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean unlock(UUID datasetId, String flightId) {
        // update the dataset entry to remove the flightid IF it is currently set to this flightid
        String sql = "UPDATE dataset SET flightid = NULL " +
            "WHERE id = :datasetid AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("datasetid", datasetId)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        logger.debug("numRowsUpdated=" + numRowsUpdated);
        return (numRowsUpdated == 1);
    }

    /**
     * Create a new dataset object and lock it. An exception is thrown if the dataset already exists.
     * The correct order to call the DatasetDao methods when creating a dataset is: createAndLock, unlock.
     * @param dataset the dataset object to create
     * @return the id of the new dataset
     * @throws SQLException
     * @throws IOException
     * @throws InvalidDatasetException if a row already exists with this dataset name
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public UUID createAndLock(Dataset dataset, String flightId) throws IOException, SQLException {
        logger.debug("createAndLock dataset " + dataset.getName());
        String sql = "INSERT INTO dataset (name, default_profile_id, flightid, description, additional_profile_ids) " +
            "VALUES (:name, :default_profile_id, :flightid, :description, :additional_profile_ids) ";
        Array additionalProfileIds = DaoUtils.createSqlUUIDArray(connection, dataset.getAdditionalProfileIds());
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", dataset.getName())
            .addValue("default_profile_id", dataset.getDefaultProfileId())
            .addValue("flightid", flightId)
            .addValue("description", dataset.getDescription())
            .addValue("additional_profile_ids", additionalProfileIds);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        try {
            jdbcTemplate.update(sql, params, keyHolder);
        } catch (DuplicateKeyException dkEx) {
            throw new InvalidDatasetException("Dataset name already exists: " + dataset.getName(), dkEx);
        }

        UUID datasetId = keyHolder.getId();
        dataset.id(datasetId);
        dataset.createdDate(keyHolder.getCreatedDate());

        tableDao.createTables(dataset.getId(), dataset.getTables());
        relationshipDao.createDatasetRelationships(dataset);
        assetDao.createAssets(dataset);

        return datasetId;
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
            String sql = "SELECT " +
                "id, name, description, default_profile_id, additional_profile_ids, created_date, flightid " +
                "FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for id " + id.toString());
        }
    }

    public DatasetSummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT " +
                "id, name, description, default_profile_id, additional_profile_ids, created_date, flightid " +
                "FROM dataset WHERE name = :name";
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
        String sql = "SELECT " +
            "id, name, description, default_profile_id, additional_profile_ids, created_date, flightid " +
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
                    .additionalProfileIds(DaoUtils.getUUIDList(rs, "additional_profile_ids"))
                    .createdDate(rs.getTimestamp("created_date").toInstant())
                    .flightId(rs.getString("flightid"));
        }
    }
}
