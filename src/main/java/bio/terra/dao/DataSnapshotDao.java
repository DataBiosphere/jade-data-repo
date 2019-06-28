package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.DataSnapshotNotFoundException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
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
    private final StudyDao studyDao;

    @Autowired
    public DataSnapshotDao(NamedParameterJdbcTemplate jdbcTemplate,
                           DataSnapshotTableDao dataSnapshotTableDao,
                           DataSnapshotMapTableDao dataSnapshotMapTableDao,
                           StudyDao studyDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.dataSnapshotTableDao = dataSnapshotTableDao;
        this.dataSnapshotMapTableDao = dataSnapshotMapTableDao;
        this.studyDao = studyDao;
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
        UUID datasetId = keyHolder.getId();
        dataSnapshot
            .id(datasetId)
            .createdDate(keyHolder.getCreatedDate());
        dataSnapshotTableDao.createTables(datasetId, dataSnapshot.getTables());

        for (DataSnapshotSource dataSnapshotSource : dataSnapshot.getDataSnapshotSources()) {
            createDataSnapshotSource(dataSnapshotSource);
        }

        return datasetId;
    }

    private void createDataSnapshotSource(DataSnapshotSource dataSnapshotSource) {
        String sql = "INSERT INTO dataset_source (dataset_id, study_id, asset_id)" +
                " VALUES (:dataset_id, :study_id, :asset_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dataset_id", dataSnapshotSource.getDataSnapshot().getId())
                .addValue("study_id", dataSnapshotSource.getStudy().getId())
                .addValue("asset_id", dataSnapshotSource.getAssetSpecification().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID id = keyHolder.getId();
        dataSnapshotSource.id(id);
        dataSnapshotMapTableDao.createTables(id, dataSnapshotSource.getDataSnapshotMapTables());
    }

    public boolean delete(UUID id) {
        logger.debug("delete dataset by id: " + id);
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    public boolean deleteByName(String datasetName) {
        logger.debug("delete dataset by name: " + datasetName);
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE name = :name",
                new MapSqlParameterSource().addValue("name", datasetName));
        return rowsAffected > 0;
    }

    public DataSnapshot retrieveDataSnapshot(UUID datasetId) {
        logger.debug("retrieve dataSnapshot id: " + datasetId);

        String sql = "SELECT id, name, description, created_date FROM dataSnapshot WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", datasetId);
        DataSnapshot dataSnapshot = retrieveWorker(sql, params);
        if (dataSnapshot == null) {
            throw new DataSnapshotNotFoundException("DataSnapshot not found - id: " + datasetId);
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
                dataSnapshot.datasetTables(dataSnapshotTableDao.retrieveTables(dataSnapshot.getId()));

                // Must be done after we we make the data snapshot tables so we can resolve the table and column references
                dataSnapshot.datasetSources(retrieveDataSnapshotSources(dataSnapshot));
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
            private UUID studyId;
            private UUID assetId;
        }

        String sql = "SELECT id, study_id, asset_id FROM dataset_source WHERE dataset_id = :dataset_id";
        List<RawSourceData> rawList =
            jdbcTemplate.query(
                sql,
                new MapSqlParameterSource().addValue("dataset_id", dataSnapshot.getId()),
                (rs, rowNum) -> {
                    RawSourceData raw = new RawSourceData();
                    raw.id = UUID.fromString(rs.getString("id"));
                    raw.studyId = rs.getObject("study_id", UUID.class);
                    raw.assetId = rs.getObject("asset_id", UUID.class);
                    return raw;
                });

        List<DataSnapshotSource> dataSnapshotSources = new ArrayList<>();
        for (RawSourceData raw : rawList) {
            Study study = studyDao.retrieve(raw.studyId);

            // Find the matching asset in the study
            Optional<AssetSpecification> assetSpecification = study.getAssetSpecificationById(raw.assetId);
            if (!assetSpecification.isPresent()) {
                throw new CorruptMetadataException("Asset referenced by dataSnapshot source was not found!");
            }

            DataSnapshotSource dataSnapshotSource = new DataSnapshotSource()
                    .id(raw.id)
                    .dataset(dataSnapshot)
                    .study(study)
                    .assetSpecification(assetSpecification.get());

            // Now that we have access to all of the parts, build the map structure
            dataSnapshotSource.datasetMapTables(dataSnapshotMapTableDao.retrieveMapTables(dataSnapshot, dataSnapshotSource));

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
        logger.debug("retrieve datasets offset: " + offset + " limit: " + limit + " sort: " + sort +
            " direction: " + direction + " filter:" + filter);
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();

        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }

        String sql = "SELECT id, name, description, created_date FROM dataset " + whereSql +
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
        sql = "SELECT count(id) AS total FROM dataset";
        params = new MapSqlParameterSource();
        Integer total = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return new MetadataEnumeration<DataSnapshotSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    public DataSnapshotSummary retrieveDataSnapshotSummary(UUID id) {
        logger.debug("retrieve dataset summary for id: " + id);
        try {
            String sql = "SELECT id, name, description, created_date FROM dataset WHERE id = :id";
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

    public List<DataSnapshotSummary> retrieveDataSnapshotsForStudy(UUID studyId) {
        try {
            String sql = "SELECT dataset.id, name, description, created_date FROM dataset " +
                "JOIN dataset_source ON dataset.id = dataset_source.dataset_id " +
                "WHERE dataset_source.study_id = :studyId";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("studyId", studyId);
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
            //this is ok - used during study delete to validate no data snapshots reference the study
            return Collections.emptyList();
        }

    }

}
