package bio.terra.service.snapshot;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.common.MetadataEnumeration;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.exception.MissingRowCountsException;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SnapshotDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.snapshot.SnapshotDao");

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final SnapshotTableDao snapshotTableDao;
    private final SnapshotMapTableDao snapshotMapTableDao;
    private final SnapshotRelationshipDao snapshotRelationshipDao;
    private final DatasetDao datasetDao;

    @Autowired
    public SnapshotDao(NamedParameterJdbcTemplate jdbcTemplate,
                      SnapshotTableDao snapshotTableDao,
                      SnapshotMapTableDao snapshotMapTableDao,
                      SnapshotRelationshipDao snapshotRelationshipDao,
                      DatasetDao datasetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.snapshotTableDao = snapshotTableDao;
        this.snapshotMapTableDao = snapshotMapTableDao;
        this.snapshotRelationshipDao = snapshotRelationshipDao;
        this.datasetDao = datasetDao;
    }

    /**
     * Lock the snapshot object before doing something with it (e.g. delete).
     * This method returns successfully when there is a snapshot object locked by this flight, and throws an exception
     * in all other cases. So, multiple locks can succeed with no errors. Logic flow of the method:
     *     1. Update the snapshot record to give this flight the lock.
     *     2. Throw an exception if no records were updated.
     * @param snapshotId id of the snapshot to lock, this is a unique column
     * @param flightId flight id that wants to lock the snapshot
     * @throws SnapshotLockException if the snapshot is locked by another flight
     * @throws SnapshotNotFoundException if the snapshot does not exist
     */
    @Transactional(propagation =  Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void lock(UUID snapshotId, String flightId) {
        if (flightId == null) {
            throw new SnapshotLockException("Locking flight id cannot be null");
        }

        // update the snapshot entry and lock it by setting the flight id
        String sql = "UPDATE snapshot SET flightid = :flightid " +
            "WHERE id = :id AND (flightid IS NULL OR flightid = :flightid)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", snapshotId)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);

        // if no rows were updated, then throw an exception
        if (numRowsUpdated == 0) {
            // this method checks if the snapshot exists
            // if it does not exist, then the method throws a SnapshotNotFoundException
            // we don't need the result (snapshot summary) here, just the existence check, so ignore the return value.
            retrieveSummaryById(snapshotId);

            // otherwise, throw a Lock exception
            logger.debug("numRowsUpdated=" + numRowsUpdated);
            throw new SnapshotLockException("Failed to lock the snapshot");
        }
    }

    /**
     * Unlock the snapshot object when finished doing something with it (e.g. delete).
     * If the snapshot is not locked by this flight, then the method is a no-op. It does not throw an exception in this
     * case. So, multiple unlocks can succeed with no errors. The method does return a boolean indicating whether any
     * rows were updated or not. So, callers can decide to throw an error if the unlock was a no-op.
     * @param snapshotId id of the snapshot to unlock, this is a unique column
     * @param flightId flight id that wants to unlock the snapshot
     * @return true if a snapshot was unlocked, false otherwise
     */
    @Transactional(propagation =  Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean unlock(UUID snapshotId, String flightId) {
        // update the snapshot entry to remove the flightid IF it is currently set to this flightid
        String sql = "UPDATE snapshot SET flightid = NULL " +
            "WHERE id = :id AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", snapshotId)
            .addValue("flightid", flightId);
        int numRowsUpdated = jdbcTemplate.update(sql, params);
        logger.debug("numRowsUpdated=" + numRowsUpdated);
        return (numRowsUpdated == 1);
    }

    /**
     * Create a new snapshot object and lock it. An exception is thrown if the snapshot already exists.
     * The correct order to call the SnapshotDao methods when creating a snapshot is: createAndLock, unlock.
     * @param snapshot the snapshot object to create
     * @return the id of the new snapshot
     * @throws InvalidSnapshotException if a row already exists with this snapshot name
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public UUID createAndLock(Snapshot snapshot, String flightId) {
        logger.debug("createAndLock snapshot " + snapshot.getName());

        String sql = "INSERT INTO snapshot (name, description, profile_id, flightid) " +
            "VALUES (:name, :description, :profile_id, :flightid) ";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", snapshot.getName())
            .addValue("description", snapshot.getDescription())
            .addValue("profile_id", snapshot.getProfileId())
            .addValue("flightid", flightId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        try {
            jdbcTemplate.update(sql, params, keyHolder);
        } catch (DuplicateKeyException dkEx) {
            throw new InvalidSnapshotException("Snapshot name already exists: " + snapshot.getName(), dkEx);
        }

        UUID snapshotId = keyHolder.getId();
        snapshot
            .id(snapshotId)
            .createdDate(keyHolder.getCreatedDate());

        snapshotTableDao.createTables(snapshotId, snapshot.getTables());
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            createSnapshotSource(snapshotSource);
        }

        return snapshotId;
    }

    /**
     * This method is protected because it's for use in tests only.
     * Currently, we don't expose the lock state of a snapshot outside of the DAO for other API code to consume.
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
        String sql = "INSERT INTO snapshot_source (snapshot_id, dataset_id, asset_id)" +
                " VALUES (:snapshot_id, :dataset_id, :asset_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshot_id", snapshotSource.getSnapshot().getId())
                .addValue("dataset_id", snapshotSource.getDataset().getId());
        if (snapshotSource.getAssetSpecification() != null) {
            params
                .addValue("asset_id", snapshotSource.getAssetSpecification().getId());
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

    public boolean delete(UUID id) {
        logger.debug("delete snapshot by id: " + id);
        int rowsAffected = jdbcTemplate.update("DELETE FROM snapshot WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    public boolean deleteByName(String snapshotName) {
        logger.debug("delete snapshot by name: " + snapshotName);
        int rowsAffected = jdbcTemplate.update("DELETE FROM snapshot WHERE name = :name",
                new MapSqlParameterSource().addValue("name", snapshotName));
        return rowsAffected > 0;
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean deleteByNameAndFlight(String snapshotName, String flightId) {
        String sql = "DELETE FROM snapshot WHERE name = :name AND flightid = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", snapshotName)
            .addValue("flightid", flightId);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    /**
     * This is a convenience wrapper that returns a snapshot only if it is NOT exclusively locked.
     * This method is intended for user-facing API calls (e.g. from RepositoryApiController).
     * @param snapshotId the snapshot id
     * @return the Snapshot object
     */
    public Snapshot retrieveAvailableSnapshot(UUID snapshotId) {
        return retrieveSnapshot(snapshotId, true);
    }

    /**
     * This is a convenience wrapper that returns a snapshot, regardless of whether it is exclusively locked.
     * Most places in the API code that are retrieving a snapshot will call this method.
     * @param snapshotId the snapshot id
     * @return the Snapshot object
     */
    public Snapshot retrieveSnapshot(UUID snapshotId) {
        return retrieveSnapshot(snapshotId, false);
    }

    /**
     * Retrieves a Snapshot object from the snapshot id.
     * @param snapshotId the snapshot id
     * @param onlyRetrieveAvailable true to exclude snapshots that are exclusively locked,
     *                              false to include all snapshots
     * @return the Snapshot object
     */
    public Snapshot retrieveSnapshot(UUID snapshotId, boolean onlyRetrieveAvailable) {
        logger.debug("retrieve snapshot id: " + snapshotId);
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

    public Snapshot retrieveSnapshotByName(String name) {
        String sql = "SELECT * FROM snapshot WHERE name = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
        Snapshot snapshot = retrieveWorker(sql, params);
        if (snapshot == null) {
            throw new SnapshotNotFoundException("Snapshot not found - name: '" + name + "'");
        }
        return snapshot;
    }

    private Snapshot retrieveWorker(String sql, MapSqlParameterSource params) {
        try {
            Snapshot snapshot = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                new Snapshot()
                    .id(rs.getObject("id", UUID.class))
                    .name(rs.getString("name"))
                    .description(rs.getString("description"))
                    .createdDate(rs.getTimestamp("created_date").toInstant())
                    .profileId(rs.getObject("profile_id", UUID.class)));
            // needed for findbugs. but really can't be null
            if (snapshot != null) {
                // retrieve the snapshot tables and relationships
                snapshot.snapshotTables(snapshotTableDao.retrieveTables(snapshot.getId()));
                snapshotRelationshipDao.retrieve(snapshot);

                // Must be done after we we make the snapshot tables so we can resolve the table and column references
                snapshot.snapshotSources(retrieveSnapshotSources(snapshot));
            }
            return snapshot;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    private List<SnapshotSource> retrieveSnapshotSources(Snapshot snapshot) {
        // We collect all of the source ids first to avoid introducing a recursive query. While the recursive
        // query might work, it makes debugging errors more difficult.
        class RawSourceData {
            private UUID id;
            private UUID datasetId;
            private UUID assetId;
        }

        String sql = "SELECT id, dataset_id, asset_id FROM snapshot_source WHERE snapshot_id = :snapshot_id";
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
            SnapshotSource snapshotSource = new SnapshotSource()
                .id(raw.id)
                .snapshot(snapshot)
                .dataset(dataset);

            if (raw.assetId != null) { // if there is no assetId, then dont check for a spec
                // Find the matching asset in the dataset
                Optional<AssetSpecification> assetSpecification = dataset.getAssetSpecificationById(raw.assetId);
                if (!assetSpecification.isPresent()) {
                    throw new CorruptMetadataException("Asset referenced by snapshot source was not found!");
                }
                snapshotSource.assetSpecification(assetSpecification.get());
            }

            // Now that we have access to all of the parts, build the map structure
            snapshotSource.snapshotMapTables(snapshotMapTableDao.retrieveMapTables(snapshot, snapshotSource));

            snapshotSources.add(snapshotSource);
        }

        return snapshotSources;
    }

    /**
     * Fetch a list of all the available snapshots.
     * This method returns summary objects, which do not include sub-objects associated with snapshots (e.g. tables).
     * Note that this method will only return snapshots that are NOT exclusively locked.
     * @param offset skip this many snapshots from the beginning of the list (intended for "scrolling" behavior)
     * @param limit only return this many snapshots in the list
     * @param sort field for order by clause. possible values are: name, description, created_date
     * @param direction asc or desc
     * @param filter string to match (SQL ILIKE) in snapshots name or description
     * @param accessibleSnapshotIds list of snapshots ids that caller has access to (fetched from IAM service)
     * @return a list of dataset summary objects
     */
    public MetadataEnumeration<SnapshotSummary> retrieveSnapshots(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> accessibleSnapshotIds) {
        logger.debug("retrieve snapshots offset: " + offset + " limit: " + limit + " sort: " + sort +
            " direction: " + direction + " filter:" + filter);
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        DaoUtils.addAuthzIdsClause(accessibleSnapshotIds, params, whereClauses);
        whereClauses.add(" flightid IS NULL"); // exclude snapshots that are exclusively locked

        // add the filter to the clause to get the actual items
        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }

        // get total count of objects
        String countSql = "SELECT count(id) AS total FROM snapshot " + whereSql;
        Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);

        String sql = "SELECT id, name, description, created_date, profile_id FROM snapshot " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<SnapshotSummary> summaries = jdbcTemplate.query(sql, params, new SnapshotSummaryMapper());

        return new MetadataEnumeration<SnapshotSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    public SnapshotSummary retrieveSummaryById(UUID id) {
        logger.debug("retrieve snapshot summary for id: " + id);
        try {
            String sql = "SELECT * FROM snapshot WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new SnapshotSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new SnapshotNotFoundException("Snapshot not found - id: " + id);
        }
    }

    public SnapshotSummary retrieveSummaryByName(String name) {
        logger.debug("retrieve snapshot summary for name: " + name);
        try {
            String sql = "SELECT * FROM snapshot WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new SnapshotSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new SnapshotNotFoundException("Snapshot not found - name: " + name);
        }
    }

    public List<SnapshotSummary> retrieveSnapshotsForDataset(UUID datasetId) {
        try {
            String sql = "SELECT snapshot.id, name, description, created_date, profile_id FROM snapshot " +
                "JOIN snapshot_source ON snapshot.id = snapshot_source.snapshot_id " +
                "WHERE snapshot_source.dataset_id = :datasetId";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("datasetId", datasetId);
            return jdbcTemplate.query(sql, params, new SnapshotSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            //this is ok - used during dataset delete to validate no snapshots reference the dataset
            return Collections.emptyList();
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void updateSnapshotTableRowCounts(Snapshot snapshot, Map<String, Long> tableRowCounts) {
        String sql = "UPDATE snapshot_table SET row_count = :rowCount " +
            "WHERE parent_id = :snapshotId AND name = :tableName";
        for (SnapshotTable snapshotTable : snapshot.getTables()) {
            String tableName = snapshotTable.getName();
            if (!tableRowCounts.containsKey(tableName)) {
                throw new MissingRowCountsException("Missing counts for " + tableName);
            }
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("rowCount", tableRowCounts.get(tableName))
                .addValue("snapshotId", snapshot.getId())
                .addValue("tableName", tableName);
            jdbcTemplate.update(sql, params);
        }
    }

    private static class SnapshotSummaryMapper implements RowMapper<SnapshotSummary> {
        public SnapshotSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new SnapshotSummary()
                .id(UUID.fromString(rs.getString("id")))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant())
                .profileId(rs.getObject("profile_id", UUID.class));
        }
    }
}
