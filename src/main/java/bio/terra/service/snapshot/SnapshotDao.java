package bio.terra.service.snapshot;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.common.MetadataEnumeration;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SnapshotDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.snapshot.SnapshotDao");

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final SnapshotTableDao snapshotTableDao;
    private final SnapshotMapTableDao snapshotMapTableDao;
    private final DatasetDao datasetDao;

    @Autowired
    public SnapshotDao(NamedParameterJdbcTemplate jdbcTemplate,
                      SnapshotTableDao snapshotTableDao,
                      SnapshotMapTableDao snapshotMapTableDao,
                      DatasetDao datasetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.snapshotTableDao = snapshotTableDao;
        this.snapshotMapTableDao = snapshotMapTableDao;
        this.datasetDao = datasetDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Snapshot snapshot) {
        logger.debug("create snapshot " + snapshot.getName());
        String sql = "INSERT INTO snapshot (name, description, profile_id)" +
                " VALUES (:name, :description, :profile_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", snapshot.getName())
                .addValue("description", snapshot.getDescription())
                .addValue("profile_id", snapshot.getProfileId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
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

    private void createSnapshotSource(SnapshotSource snapshotSource) {
        String sql = "INSERT INTO snapshot_source (snapshot_id, dataset_id, asset_id)" +
                " VALUES (:snapshot_id, :dataset_id, :asset_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshot_id", snapshotSource.getSnapshot().getId())
                .addValue("dataset_id", snapshotSource.getDataset().getId())
                .addValue("asset_id", snapshotSource.getAssetSpecification().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID id = keyHolder.getId();
        snapshotSource.id(id);
        snapshotMapTableDao.createTables(id, snapshotSource.getSnapshotMapTables());
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

    public Snapshot retrieveSnapshot(UUID snapshotId) {
        logger.debug("retrieve snapshot id: " + snapshotId);
        String sql = "SELECT * FROM snapshot WHERE id = :id";
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
                // retrieve the snapshot tables
                snapshot.snapshotTables(snapshotTableDao.retrieveTables(snapshot.getId()));

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

            // Find the matching asset in the dataset
            Optional<AssetSpecification> assetSpecification = dataset.getAssetSpecificationById(raw.assetId);
            if (!assetSpecification.isPresent()) {
                throw new CorruptMetadataException("Asset referenced by snapshot source was not found!");
            }

            SnapshotSource snapshotSource = new SnapshotSource()
                    .id(raw.id)
                    .snapshot(snapshot)
                    .dataset(dataset)
                    .assetSpecification(assetSpecification.get());

            // Now that we have access to all of the parts, build the map structure
            snapshotSource.snapshotMapTables(snapshotMapTableDao.retrieveMapTables(snapshot, snapshotSource));

            snapshotSources.add(snapshotSource);
        }

        return snapshotSources;
    }

    public MetadataEnumeration<SnapshotSummary> retrieveSnapshots(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> accessibleDatasetIds) {
        logger.debug("retrieve snapshots offset: " + offset + " limit: " + limit + " sort: " + sort +
            " direction: " + direction + " filter:" + filter);
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        DaoUtils.addAuthzIdsClause(accessibleDatasetIds, params, whereClauses);

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

    public SnapshotSummary retrieveSnapshotSummary(UUID id) {
        logger.debug("retrieve snapshot summary for id: " + id);
        try {
            String sql = "SELECT * FROM snapshot WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new SnapshotSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new SnapshotNotFoundException("Snapshot not found - id: " + id);
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
