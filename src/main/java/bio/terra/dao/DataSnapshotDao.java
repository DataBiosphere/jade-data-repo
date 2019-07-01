package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.DataSnapshotNotFoundException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.DrDataset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class DataSnapshotDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.dao.DataSnapshotDao");

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DataSnapshotTableDao dataSnapshotTableDao;
    private final DataSnapshotMapTableDao dataSnapshotMapTableDao;
    private final DrDatasetDao datasetDao;

    @Autowired
    public DataSnapshotDao(NamedParameterJdbcTemplate jdbcTemplate,
                           DataSnapshotTableDao dataSnapshotTableDao,
                           DataSnapshotMapTableDao dataSnapshotMapTableDao,
                           DrDatasetDao datasetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.dataSnapshotTableDao = dataSnapshotTableDao;
        this.dataSnapshotMapTableDao = dataSnapshotMapTableDao;
        this.datasetDao = datasetDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(DataSnapshot dataSnapshot) {
        logger.debug("create dataSnapshot " + dataSnapshot.getName());
        String sql = "INSERT INTO dataSnapshot (name, description)" +
                " VALUES (:name, :description)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", dataSnapshot.getName())
                .addValue("description", dataSnapshot.getDescription());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID dataSnapshotId = keyHolder.getId();
        dataSnapshot
            .id(dataSnapshotId)
            .createdDate(keyHolder.getCreatedDate());
        dataSnapshotTableDao.createTables(dataSnapshotId, dataSnapshot.getTables());

        for (DataSnapshotSource dataSnapshotSource : dataSnapshot.getDataSnapshotSources()) {
            createDataSnapshotSource(dataSnapshotSource);
        }

        return dataSnapshotId;
    }

    private void createDataSnapshotSource(DataSnapshotSource dataSnapshotSource) {
        String sql = "INSERT INTO datasnapshot_source (datasnapshot_id, dataset_id, asset_id)" +
                " VALUES (:datasnapshot_id, :dataset_id, :asset_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("datasnapshot_id", dataSnapshotSource.getDataSnapshot().getId())
                .addValue("dataset_id", dataSnapshotSource.getDataset().getId())
                .addValue("asset_id", dataSnapshotSource.getAssetSpecification().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID id = keyHolder.getId();
        dataSnapshotSource.id(id);
        dataSnapshotMapTableDao.createTables(id, dataSnapshotSource.getDataSnapshotMapTables());
    }

    public boolean delete(UUID id) {
        logger.debug("delete dataSnapshot by id: " + id);
        int rowsAffected = jdbcTemplate.update("DELETE FROM datasnapshot WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    public boolean deleteByName(String dataSnapshotName) {
        logger.debug("delete dataSnapshot by name: " + dataSnapshotName);
        int rowsAffected = jdbcTemplate.update("DELETE FROM datasnapshot WHERE name = :name",
                new MapSqlParameterSource().addValue("name", dataSnapshotName));
        return rowsAffected > 0;
    }

    public DataSnapshot retrieveDataSnapshot(UUID dataSnapshotId) {
        logger.debug("retrieve dataSnapshot id: " + dataSnapshotId);

        String sql = "SELECT id, name, description, created_date FROM dataSnapshot WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", dataSnapshotId);
        DataSnapshot dataSnapshot = retrieveWorker(sql, params);
        if (dataSnapshot == null) {
            throw new DataSnapshotNotFoundException("DataSnapshot not found - id: " + dataSnapshotId);
        }
        return dataSnapshot;
    }

    public DataSnapshot retrieveDataSnapshotByName(String name) {
        String sql = "SELECT id, name, description, created_date FROM dataSnapshot WHERE name = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
        DataSnapshot dataSnapshot = retrieveWorker(sql, params);
        if (dataSnapshot == null) {
            throw new DataSnapshotNotFoundException("DataSnapshot not found - name: '" + name + "'");
        }
        return dataSnapshot;
    }

    private DataSnapshot retrieveWorker(String sql, MapSqlParameterSource params) {
        try {
            DataSnapshot dataSnapshot = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    new DataSnapshot()
                            .id(rs.getObject("id", UUID.class))
                            .name(rs.getString("name"))
                            .description(rs.getString("description"))
                            .createdDate(rs.getTimestamp("created_date").toInstant()));
            // needed for fix bugs. but really can't be null
            if (dataSnapshot != null) {
                // retrieve the data snapshot tables
                dataSnapshot.dataSnapshotTables(dataSnapshotTableDao.retrieveTables(dataSnapshot.getId()));

                // Must be done after we we make the data snapshot tables so we can resolve the table and column refs
                dataSnapshot.dataSnapshotSources(retrieveDataSnapshotSources(dataSnapshot));
            }
            return dataSnapshot;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    private List<DataSnapshotSource> retrieveDataSnapshotSources(DataSnapshot dataSnapshot) {
        // We collect all of the source ids first to avoid introducing a recursive query. While the recursive
        // query might work, it makes debugging errors more difficult.
        class RawSourceData {
            private UUID id;
            private UUID datasetId;
            private UUID assetId;
        }

        String sql = "SELECT id, dataset_id, asset_id FROM datasnapshot_source " +
            " WHERE datasnapshot_id = :datasnapshot_id";
        List<RawSourceData> rawList =
            jdbcTemplate.query(
                sql,
                new MapSqlParameterSource().addValue("datasnapshot_id", dataSnapshot.getId()),
                (rs, rowNum) -> {
                    RawSourceData raw = new RawSourceData();
                    raw.id = UUID.fromString(rs.getString("id"));
                    raw.datasetId = rs.getObject("dataset_id", UUID.class);
                    raw.assetId = rs.getObject("asset_id", UUID.class);
                    return raw;
                });

        List<DataSnapshotSource> dataSnapshotSources = new ArrayList<>();
        for (RawSourceData raw : rawList) {
            DrDataset dataset = datasetDao.retrieve(raw.datasetId);

            // Find the matching asset in the dataset
            Optional<AssetSpecification> assetSpecification = dataset.getAssetSpecificationById(raw.assetId);
            if (!assetSpecification.isPresent()) {
                throw new CorruptMetadataException("Asset referenced by dataSnapshot source was not found!");
            }

            DataSnapshotSource dataSnapshotSource = new DataSnapshotSource()
                    .id(raw.id)
                    .dataSnapshot(dataSnapshot)
                    .dataset(dataset)
                    .assetSpecification(assetSpecification.get());

            // Now that we have access to all of the parts, build the map structure
            dataSnapshotSource.dataSnapshotMapTables(
                dataSnapshotMapTableDao.retrieveMapTables(dataSnapshot, dataSnapshotSource));

            dataSnapshotSources.add(dataSnapshotSource);
        }

        return dataSnapshotSources;
    }

    public MetadataEnumeration<DataSnapshotSummary> retrieveDataSnapshots(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter
    ) {
        logger.debug("retrieve dataSnapshots offset: " + offset + " limit: " + limit + " sort: " + sort +
            " direction: " + direction + " filter:" + filter);
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();

        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }

        String sql = "SELECT id, name, description, created_date FROM datasnapshot " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<DataSnapshotSummary> summaries = jdbcTemplate.query(sql, params, (rs, rowNum) -> {
            DataSnapshotSummary summary = new DataSnapshotSummary()
                .id(UUID.fromString(rs.getString("id")))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant());
            return summary;
        });
        sql = "SELECT count(id) AS total FROM datasnapshot";
        params = new MapSqlParameterSource();
        Integer total = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return new MetadataEnumeration<DataSnapshotSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    public DataSnapshotSummary retrieveDataSnapshotSummary(UUID id) {
        logger.debug("retrieve dataSnapshot summary for id: " + id);
        try {
            String sql = "SELECT id, name, description, created_date FROM datasnapshot WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);

            DataSnapshotSummary dataSnapshotSummary = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    new DataSnapshotSummary()
                            .id(rs.getObject("id", UUID.class))
                            .name(rs.getString("name"))
                            .description(rs.getString("description"))
                            .createdDate(rs.getTimestamp("created_date").toInstant()));
            return dataSnapshotSummary;
        } catch (EmptyResultDataAccessException ex) {
            throw new DataSnapshotNotFoundException("DataSnapshot not found - id: " + id);
        }
    }

    public List<DataSnapshotSummary> retrieveDataSnapshotsForDataset(UUID datasetId) {
        try {
            String sql = "SELECT datasnapshot.id, name, description, created_date FROM datasnapshot " +
                "JOIN datasnapshot_source ON datasnapshot.id = datasnapshot_source.datasnapshot_id " +
                "WHERE datasnapshot_source.dataset_id = :datasetId";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("datasetId", datasetId);
            List<DataSnapshotSummary> summaries = jdbcTemplate.query(
                sql,
                params,
                (rs, rowNum) -> {
                    DataSnapshotSummary summary = new DataSnapshotSummary()
                        .id(UUID.fromString(rs.getString("id")))
                        .name(rs.getString("name"))
                        .description(rs.getString("description"))
                        .createdDate(rs.getTimestamp("created_date").toInstant());
                    return summary;
                });
            return summaries;
        } catch (EmptyResultDataAccessException ex) {
            //this is ok - used during dataset delete to validate no data snapshots reference the dataset
            return Collections.emptyList();
        }

    }

}
