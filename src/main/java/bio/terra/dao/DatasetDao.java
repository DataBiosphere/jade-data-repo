package bio.terra.dao;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Repository
public class DatasetDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DatasetTableDao datasetTableDao;
    private final DatasetMapTableDao datasetMapTableDao;

    @Autowired
    public DatasetDao(NamedParameterJdbcTemplate jdbcTemplate,
                      DatasetTableDao datasetTableDao,
                      DatasetMapTableDao datasetMapTableDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.datasetTableDao = datasetTableDao;
        this.datasetMapTableDao = datasetMapTableDao;
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
        dataset.id(datasetId);
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

}
