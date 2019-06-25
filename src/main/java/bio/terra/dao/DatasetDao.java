package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.resourcemanagement.service.ProfileService;
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
public class DatasetDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.dao.DatasetDao");

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DatasetTableDao datasetTableDao;
    private final DatasetMapTableDao datasetMapTableDao;
    private final StudyDao studyDao;

    @Autowired
    public DatasetDao(NamedParameterJdbcTemplate jdbcTemplate,
                      DatasetTableDao datasetTableDao,
                      DatasetMapTableDao datasetMapTableDao,
                      StudyDao studyDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.datasetTableDao = datasetTableDao;
        this.datasetMapTableDao = datasetMapTableDao;
        this.studyDao = studyDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Dataset dataset) {
        logger.debug("create dataset " + dataset.getName());
        String sql = "INSERT INTO dataset (name, description, profile_id)" +
                " VALUES (:name, :description, :profile_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", dataset.getName())
                .addValue("description", dataset.getDescription())
                .addValue("profile_id", dataset.getProfileId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID datasetId = keyHolder.getId();
        dataset
            .id(datasetId)
            .createdDate(keyHolder.getCreatedDate());
        datasetTableDao.createTables(datasetId, dataset.getTables());

        for (DatasetSource datasetSource : dataset.getDatasetSources()) {
            createDatasetSource(datasetSource);
        }

        return datasetId;
    }

    private void createDatasetSource(DatasetSource datasetSource) {
        String sql = "INSERT INTO dataset_source (dataset_id, study_id, asset_id)" +
                " VALUES (:dataset_id, :study_id, :asset_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dataset_id", datasetSource.getDataset().getId())
                .addValue("study_id", datasetSource.getStudy().getId())
                .addValue("asset_id", datasetSource.getAssetSpecification().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID id = keyHolder.getId();
        datasetSource.id(id);
        datasetMapTableDao.createTables(id, datasetSource.getDatasetMapTables());
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

    public Dataset retrieveDataset(UUID datasetId) {
        logger.debug("retrieve dataset id: " + datasetId);
        String sql = "SELECT * FROM dataset WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", datasetId);
        Dataset dataset = retrieveWorker(sql, params);
        if (dataset == null) {
            throw new DatasetNotFoundException("Dataset not found - id: " + datasetId);
        }
        return dataset;
    }

    public Dataset retrieveDatasetByName(String name) {
        String sql = "SELECT * FROM dataset WHERE name = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
        Dataset dataset = retrieveWorker(sql, params);
        if (dataset == null) {
            throw new DatasetNotFoundException("Dataset not found - name: '" + name + "'");
        }
        return dataset;
    }

    private Dataset retrieveWorker(String sql, MapSqlParameterSource params) {
        try {
            Dataset dataset = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                new Dataset()
                        .id(rs.getObject("id", UUID.class))
                        .name(rs.getString("name"))
                        .description(rs.getString("description"))
                        .createdDate(rs.getTimestamp("created_date").toInstant())
                        .profileId(rs.getObject("profile_id", UUID.class)));
            // needed for findbugs. but really can't be null
            if (dataset != null) {
                // retrieve the dataset tables
                dataset.datasetTables(datasetTableDao.retrieveTables(dataset.getId()));

                // Must be done after we we make the dataset tables so we can resolve the table and column references
                dataset.datasetSources(retrieveDatasetSources(dataset));
            }
            return dataset;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    private List<DatasetSource> retrieveDatasetSources(Dataset dataset) {
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
                new MapSqlParameterSource().addValue("dataset_id", dataset.getId()),
                (rs, rowNum) -> {
                    RawSourceData raw = new RawSourceData();
                    raw.id = UUID.fromString(rs.getString("id"));
                    raw.studyId = rs.getObject("study_id", UUID.class);
                    raw.assetId = rs.getObject("asset_id", UUID.class);
                    return raw;
                });

        List<DatasetSource> datasetSources = new ArrayList<>();
        for (RawSourceData raw : rawList) {
            Study study = studyDao.retrieve(raw.studyId);

            // Find the matching asset in the study
            Optional<AssetSpecification> assetSpecification = study.getAssetSpecificationById(raw.assetId);
            if (!assetSpecification.isPresent()) {
                throw new CorruptMetadataException("Asset referenced by dataset source was not found!");
            }

            DatasetSource datasetSource = new DatasetSource()
                    .id(raw.id)
                    .dataset(dataset)
                    .study(study)
                    .assetSpecification(assetSpecification.get());

            // Now that we have access to all of the parts, build the map structure
            datasetSource.datasetMapTables(datasetMapTableDao.retrieveMapTables(dataset, datasetSource));

            datasetSources.add(datasetSource);
        }

        return datasetSources;
    }

    public MetadataEnumeration<DatasetSummary> retrieveDatasets(
            int offset,
            int limit,
            String sort,
            String direction,
            String filter) {
        logger.debug("retrieve datasets offset: " + offset + " limit: " + limit + " sort: " + sort +
            " direction: " + direction + " filter:" + filter);
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();

        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }

        String sql = "SELECT id, name, description, created_date, profile_id FROM dataset " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<DatasetSummary> summaries = jdbcTemplate.query(sql, params, (rs, rowNum) -> {
            DatasetSummary summary = new DatasetSummary()
                .id(UUID.fromString(rs.getString("id")))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant());
            return summary;
        });
        sql = "SELECT count(id) AS total FROM dataset";
        params = new MapSqlParameterSource();
        Integer total = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return new MetadataEnumeration<DatasetSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    public DatasetSummary retrieveDatasetSummary(UUID id) {
        logger.debug("retrieve dataset summary for id: " + id);
        try {
            String sql = "SELECT * FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found - id: " + id);
        }
    }

    public List<DatasetSummary> retrieveDatasetsForStudy(UUID studyId) {
        try {
            String sql = "SELECT dataset.id, name, description, created_date FROM dataset " +
                "JOIN dataset_source ON dataset.id = dataset_source.dataset_id " +
                "WHERE dataset_source.study_id = :studyId";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("studyId", studyId);
            return jdbcTemplate.query(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            //this is ok - used during study delete to validate no datasets reference the study
            return Collections.emptyList();
        }
    }

    private static class DatasetSummaryMapper implements RowMapper<DatasetSummary> {
        public DatasetSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DatasetSummary()
                .id(UUID.fromString(rs.getString("id")))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant())
                .profileId(rs.getObject("profile_id", UUID.class));
        }
    }
}
