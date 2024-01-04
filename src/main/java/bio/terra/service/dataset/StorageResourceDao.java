package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.exception.InvalidStorageException;
import bio.terra.service.dataset.exception.StorageResourceNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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

@Repository
public class StorageResourceDao {

  private static final String STORAGE_COLUMNS =
      "dataset_id, cloud_platform, " + "cloud_resource, region ";
  private static final String SQL_GET =
      "SELECT " + STORAGE_COLUMNS + "FROM storage_resource WHERE dataset_id = :dataset_id";
  private static final String SQL_GET_LIST =
      "SELECT " + STORAGE_COLUMNS + "FROM storage_resource where dataset_id in (:dataset_ids)";
  private static final Logger logger = LoggerFactory.getLogger(StorageResourceDao.class);

  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<StorageResource<?, ?>> getStorageResourcesByDatasetId(UUID datasetId) {
    try {
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", datasetId);
      return jdbcTemplate.query(SQL_GET, params, new StorageResourceMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new StorageResourceNotFoundException(
          "Storage resource not found for dataset: " + datasetId);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<StorageResource<?, ?>> getStorageResourcesForDatasetIds(List<UUID> datasetIds) {
    if (datasetIds.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      MapSqlParameterSource params =
          new MapSqlParameterSource().addValue("dataset_ids", datasetIds);
      return jdbcTemplate.query(SQL_GET_LIST, params, new StorageResourceMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new StorageResourceNotFoundException(
          "Storage resources not found for dataset enumerate query");
    }
  }

  private static class StorageResourceMapper implements RowMapper<StorageResource<?, ?>> {
    public StorageResource<?, ?> mapRow(ResultSet rs, int rowNum) throws SQLException {
      final CloudPlatform cloudPlatform = CloudPlatform.valueOf(rs.getString("cloud_platform"));
      switch (cloudPlatform) {
        case GCP:
          return new GoogleStorageResource(
              UUID.fromString(rs.getString("dataset_id")),
              GoogleCloudResource.valueOf(rs.getString("cloud_resource")),
              GoogleRegion.valueOf(rs.getString("region")));
        case AZURE:
          return new AzureStorageResource(
              UUID.fromString(rs.getString("dataset_id")),
              AzureCloudResource.valueOf(rs.getString("cloud_resource")),
              AzureRegion.valueOf(rs.getString("region")));
        default:
          throw new InvalidCloudPlatformException();
      }
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createStorageAttributes(
      List<? extends StorageResource<?, ?>> storageResources, UUID datasetId) {
    logger.debug("Create Operation: createStorageAttributes datasetId: {}", datasetId);

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", datasetId);

    List<String> valuesList = new ArrayList<>();
    for (StorageResource<?, ?> storageResource : storageResources) {
      String regionParam = storageResource.getCloudResource() + "_region";
      String cloudResourceParam = storageResource.getCloudResource() + "_cloudResource";
      String platformParam = storageResource.getCloudResource() + "_cloudPlatform";
      valuesList.add(
          String.format(
              "(:dataset_id, :%s, :%s, :%s)", regionParam, cloudResourceParam, platformParam));

      params.addValue(regionParam, storageResource.getRegion().name());
      params.addValue(cloudResourceParam, storageResource.getCloudResource().name());
      params.addValue(platformParam, storageResource.getCloudPlatform().name());
    }

    String sql =
        "INSERT INTO storage_resource "
            + "(dataset_id, region, cloud_resource, cloud_platform) "
            + "VALUES "
            + String.join(",\n", valuesList);
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
