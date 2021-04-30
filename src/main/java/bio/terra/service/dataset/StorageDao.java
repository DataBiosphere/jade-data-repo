package bio.terra.service.dataset;

import bio.terra.model.CloudPlatform;
import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoKeyHolder;
import bio.terra.model.GoogleCloudResource;
import bio.terra.service.dataset.exception.InvalidStorageException;
import bio.terra.service.dataset.exception.StorageResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class StorageDao {

    private static final String STORAGE_COLUMNS = "id, dataset_id, cloud_platform, " +
        "cloud_resource, region ";
    private static final String SQL_GET = "SELECT " + STORAGE_COLUMNS +
        "FROM storage_resource WHERE dataset_id = :dataset_id";
    private static final String SQL_GET_BUCKET = "SELECT sr.region " +
        "FROM storage_resource sr, " +
        "dataset_bucket db " +
        "WHERE sr.dataset_id = db.dataset_id " +
        "AND db.bucket_resource_id = :bucket_resource_id " +
        "AND sr.cloud_resource = :cloud_resource";
    private static final Logger logger = LoggerFactory.getLogger(StorageDao.class);
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public StorageDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public List<StorageResource> getStorageResourcesByDatasetId(UUID datasetId) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dataset_id", datasetId);
            return jdbcTemplate.query(SQL_GET, params, new StorageResourceMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StorageResourceNotFoundException("Storage resource not found for dataset: "
                + datasetId.toString());
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public String getBucketStorageFromBucketResourceId(UUID bucketResourceId) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("bucket_resource_id", bucketResourceId)
                .addValue("cloud_resource", GoogleCloudResource.BUCKET.toString());
            return Optional.ofNullable(jdbcTemplate.queryForObject(SQL_GET_BUCKET, params, new StorageResourceMapper()))
                .map(StorageResource::getRegion).orElse(null);
        } catch (EmptyResultDataAccessException ex) {
            throw new StorageResourceNotFoundException("Storage resource not found for bucket: "
                + bucketResourceId.toString());
        }
    }

    private static class StorageResourceMapper implements RowMapper<StorageResource> {
        public StorageResource mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StorageResource()
                .cloudPlatform(CloudPlatform.fromValue(rs.getString("cloud_platform")))
                .cloudResource(rs.getString("cloud_resource"))
                .region(rs.getString("region"));
        }
    }

    @Transactional(propagation =  Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void createStorageAttributes(List<StorageResource> storageResources, UUID datasetId) {
        logger.debug("Create Operation: createStorageAttributes datasetId: {}", datasetId);

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", datasetId);

        List<String> valuesList = new ArrayList<>();
        for (StorageResource storageResource : storageResources) {
            String regionParam = storageResource.getCloudResource() + "_region";
            String cloudResourceParam = storageResource.getCloudResource() + "_cloudResource";
            String platformParam = storageResource.getCloudResource() + "_cloudPlatform";
            valuesList.add(String.format("(:dataset_id, :%s, :%s, :%s)",
                regionParam, cloudResourceParam, platformParam));
            params.addValue(regionParam, storageResource.getRegion());
            params.addValue(cloudResourceParam, storageResource.getCloudResource());
            params.addValue(platformParam, storageResource.getCloudPlatform().toString());
        }

        String sql = "INSERT INTO storage_resource " +
            "(dataset_id, region, cloud_resource, cloud_platform) " +
            "VALUES " + String.join(",\n", valuesList);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        try {
            jdbcTemplate.update(sql, params, keyHolder);
        } catch (DataAccessException daEx) {
            throw new InvalidStorageException(
                "Couldn't create storage rows for dataset id: " + datasetId, daEx);
        }

        logger.debug("end of createStorageAttributes datasetId: {}", datasetId);
    }
}
