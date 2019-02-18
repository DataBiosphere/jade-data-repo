package bio.terra.dao;

import bio.terra.dao.exceptions.CorruptMetadataException;
import bio.terra.exceptions.NotFoundException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.Study;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class DatasetDao {

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
        String sql = "INSERT INTO dataset (name, description, created_date)" +
                " VALUES (:name, :description, CURRENT_TIMESTAMP)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", dataset.getName())
                .addValue("description", dataset.getDescription());
        UUIDHolder keyHolder = new UUIDHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID datasetId = keyHolder.getId();
        Instant createdDate = keyHolder.getCreatedDate();
        dataset.id(datasetId)
                .createdDate(createdDate);
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
        UUIDHolder keyHolder = new UUIDHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID id = keyHolder.getId();
        datasetSource.id(id);
        datasetMapTableDao.createTables(id, datasetSource.getDatasetMapTables());
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

    public Dataset retrieveDataset(UUID datasetId) {
        String sql = "SELECT id, name, description, created_date FROM dataset WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", datasetId);
        Dataset dataset = retrieveWorker(sql, params);
        if (dataset == null) {
            throw new NotFoundException("Dataset not found - id: " + datasetId);
        }
        return dataset;
    }

    public Dataset retrieveDatasetByName(String name) {
        String sql = "SELECT id, name, description, created_date FROM dataset WHERE nane = :name";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
        Dataset dataset = retrieveWorker(sql, params);
        if (dataset == null) {
            throw new NotFoundException("Dataset not found - name: '" + name + "'");
        }
        return dataset;
    }

    private Dataset retrieveWorker(String sql, MapSqlParameterSource params) {
        try {
            Dataset dataset = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    new Dataset()
                            .id(UUID.fromString(rs.getString("id")))
                            .name(rs.getString("name"))
                            .description(rs.getString("description"))
                            .createdDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class))));
            // needed for fix bugs. but really can't be null
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
                    raw.studyId = UUID.fromString(rs.getString("study_id"));
                    raw.assetId = UUID.fromString(rs.getString("asset_id"));
                    return raw;
                });

        List<DatasetSource> datasetSources = new ArrayList<>();
        for (RawSourceData raw : rawList) {
            Optional<Study> study = studyDao.retrieve(raw.studyId);
            if (!study.isPresent()) {
                throw new CorruptMetadataException("Study referenced by dataset source was not found!");
            }

            // Find the matching asset in the study
            Optional<AssetSpecification> assetSpecification = study.get().getAssetSpecificationById(raw.assetId);
            if (!assetSpecification.isPresent()) {
                throw new CorruptMetadataException("Asset referenced by dataset source was not found!");
            }

            DatasetSource datasetSource = new DatasetSource()
                    .id(raw.id)
                    .dataset(dataset)
                    .study(study.get())
                    .assetSpecification(assetSpecification.get());

            // Now that we have access to all of the parts, build the map structure
            datasetSource.datasetMapTables(datasetMapTableDao.retrieveMapTables(dataset, datasetSource));

            datasetSources.add(datasetSource);
        }

        return datasetSources;
    }

    public List<DatasetSummary> retrieveDatasets(int offset, int limit) {
        String sql = "SELECT id, name, description, created_date FROM dataset" +
                " ORDER BY created_date OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("offset", offset)
                .addValue("limit", limit);
        List<DatasetSummary> summaries = jdbcTemplate.query(
            sql,
            params,
            (rs, rowNum) -> {
                DatasetSummary summary = new DatasetSummary()
                        .id(UUID.fromString(rs.getString("id")))
                        .name(rs.getString("name"))
                        .description(rs.getString("description"))
                        .createdDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class)));
                return summary;
            });
        return summaries;
    }

    public DatasetSummary retrieveDatasetSummary(UUID id) {
        try {
            String sql = "SELECT id, name, description, created_date FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);

            DatasetSummary datasetSummary = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    new DatasetSummary()
                            .id(UUID.fromString(rs.getString("id")))
                            .name(rs.getString("name"))
                            .description(rs.getString("description"))
                            .createdDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class))));
            return datasetSummary;
        } catch (EmptyResultDataAccessException ex) {
            throw new NotFoundException("Dataset not found - id: " + id);
        }
    }

}
