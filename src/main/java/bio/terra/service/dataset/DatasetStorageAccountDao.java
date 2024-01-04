package bio.terra.service.dataset;

import static bio.terra.common.DaoUtils.retryQuery;

import bio.terra.common.DaoUtils.UuidMapper;
import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/*
 * A NOTE ON CONCURRENCY CONTROL FOR THE DATASET STORAGE ACCOUNT TABLE
 * The successful_ingest counter is a concurrency control mechanism to avoid locking the dataset-storage-account row.
 * The only important values are 0 or greater than 0. A row with a counter of 0 is equivalent to no row;
 * that is, no files in the dataset are using the storage account. A value greater than 0 means the dataset is
 * using the storage account.
 *
 * An ingest either creates the row with a value of 1 or it increments the value. The undo step always
 * decrements the value. That allows parallel ingests. On failure, we do not worry about trying to
 * removed the row.
 *
 * Another alternative would be to use locking, as we do with datasets. That would require adding
 * id and flightid columns to the table. It would mean that if the row were locked, a second
 * ingest would have to wait for the completion of the first ingest flight before it could begin.
 * So I think this is a simpler way to go, at the cost of leaving rows in this table.
 */
@Repository
public class DatasetStorageAccountDao {
  private static final Logger logger = LoggerFactory.getLogger(DatasetStorageAccountDao.class);

  // Note we start from 1, since we are recording an ingest creating the link. If that ingest fails
  // it will decrement to zero.
  private static final String sqlCreateLink =
      "INSERT INTO dataset_storage_account "
          + " (dataset_id, storage_account_resource_id, successful_ingests) VALUES "
          + " (:dataset_id, :storage_account_resource_id, :counter_value)";

  private static final String whereClause =
      " WHERE dataset_id = :dataset_id AND storage_account_resource_id = :storage_account_resource_id";

  private static final String sqlIncrementCount =
      "UPDATE dataset_storage_account "
          + " SET successful_ingests = successful_ingests + 1"
          + whereClause;

  private static final String sqlDecrementCount =
      "UPDATE dataset_storage_account "
          + " SET successful_ingests = successful_ingests - 1"
          + whereClause;

  private static final String sqlExistsLink =
      "SELECT COUNT(*) FROM dataset_storage_account" + whereClause;

  private static final String sqlGetSuccessfulIngestCount =
      "SELECT successful_ingests FROM dataset_storage_account" + whereClause;

  private static final String sqlDeleteLink = "DELETE FROM dataset_storage_account" + whereClause;

  private static final String sqlGetStorageAccountResourceId =
      "SELECT storage_account_resource_id FROM dataset_storage_account WHERE dataset_id = :dataset_id";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public DatasetStorageAccountDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createDatasetStorageAccountLink(
      UUID datasetId, UUID storageAccountResourceId, boolean createdDuringFileIngest) {
    if (datasetStorageAccountLinkExists(datasetId, storageAccountResourceId)) {
      // If the link is already made then increment our use of it if we are doing a file ingest.
      if (createdDuringFileIngest) {
        incrementDatasetStorageAccountLink(datasetId, storageAccountResourceId);
      }
    } else {
      // Not there, try creating it
      int initialValue = createdDuringFileIngest ? 1 : 0;
      datasetStorageAccountLinkUpdate(
          sqlCreateLink, datasetId, storageAccountResourceId, initialValue);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteDatasetStorageAccountLink(UUID datasetId, UUID storageAccountResourceId) {
    datasetStorageAccountLinkUpdate(sqlDeleteLink, datasetId, storageAccountResourceId, null);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void decrementDatasetStorageAccountLink(UUID datasetId, UUID storageAccountResourceId) {
    datasetStorageAccountLinkUpdate(sqlDecrementCount, datasetId, storageAccountResourceId, null);
  }

  boolean datasetStorageAccountLinkExists(UUID datasetId, UUID storageAccountResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("storage_account_resource_id", storageAccountResourceId);
    Integer count = jdbcTemplate.queryForObject(sqlExistsLink, params, Integer.class);
    if (count == null) {
      throw new CorruptMetadataException("Impossible null value from count");
    }
    return (count == 1);
  }

  public List<UUID> getStorageAccountResourceIdForDatasetId(UUID datasetId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", datasetId);
    return jdbcTemplate.query(
        sqlGetStorageAccountResourceId, params, new UuidMapper("storage_account_resource_id"));
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  int datasetStorageAccountSuccessfulIngestCount(UUID datasetId, UUID storageAccountResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("storage_account_resource_id", storageAccountResourceId);
    Integer count = jdbcTemplate.queryForObject(sqlGetSuccessfulIngestCount, params, Integer.class);
    if (count == null) {
      throw new CorruptMetadataException("Impossible null value from count");
    }
    return count;
  }

  private void incrementDatasetStorageAccountLink(UUID datasetId, UUID storageAccountResourceId) {
    datasetStorageAccountLinkUpdate(sqlIncrementCount, datasetId, storageAccountResourceId, null);
  }

  private void datasetStorageAccountLinkUpdate(
      String sql, UUID datasetId, UUID storageAccountResourceId, Integer counterValue) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("storage_account_resource_id", storageAccountResourceId);
    if (counterValue != null) {
      params.addValue("counter_value", counterValue);
    }

    try {
      jdbcTemplate.update(sql, params);
    } catch (DataAccessException dataAccessException) {
      if (retryQuery(dataAccessException)) {
        logger.error("datasetStorageAccount link operation failed with retryable exception.");
        throw new RetryQueryException("Retry", dataAccessException);
      }
      logger.error("datasetStorageAccount link operation failed with fatal exception.");
      throw dataAccessException;
    }
  }
}
