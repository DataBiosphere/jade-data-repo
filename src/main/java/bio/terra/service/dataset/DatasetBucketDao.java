package bio.terra.service.dataset;

import static bio.terra.common.DaoUtils.retryQuery;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/* A NOTE ON CONCURRENCY CONTROL FOR THE DATASET BUCKET TABLE
 * The successful_ingest counter is a concurrency control mechanism to avoid locking the dataset-bucket row.
 * The only important values are 0 or greater than 0. A row with a counter of 0 is equivalent to no row;
 * that is, no files in the dataset are using the bucket. A value greater than 0 means the dataset is
 * using the bucket.
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
public class DatasetBucketDao {
  private static final Logger logger = LoggerFactory.getLogger(DatasetBucketDao.class);

  private static final String sqlGetProjectIdForBucket =
      "SELECT pr.google_project_id FROM dataset_bucket db JOIN"
          + " bucket_resource br ON db.bucket_resource_id = br.id JOIN"
          + " project_resource pr ON br.project_resource_id = pr.id"
          + " WHERE db.dataset_id = :dataset_id AND pr.profile_id = :profile_id";

  private static final String sqlGetProjectResourceForBucketPerDataset =
      "SELECT br.project_resource_id FROM dataset_bucket db JOIN"
          + " bucket_resource br ON db.bucket_resource_id = br.id"
          + " WHERE db.dataset_id = :dataset_id";

  // Note we start from 1, since we are recording an ingest creating the link. If that ingest fails
  // it will decrement to zero.
  private static final String sqlCreateLink =
      "INSERT INTO dataset_bucket "
          + " (dataset_id, bucket_resource_id, successful_ingests) VALUES (:dataset_id, :bucket_resource_id, 1)";

  private static final String whereClause =
      " WHERE dataset_id = :dataset_id AND bucket_resource_id = :bucket_resource_id";

  private static final String sqlIncrementCount =
      "UPDATE dataset_bucket " + " SET successful_ingests = successful_ingests + 1" + whereClause;

  private static final String sqlDecrementCount =
      "UPDATE dataset_bucket " + " SET successful_ingests = successful_ingests - 1" + whereClause;

  private static final String sqlExistsLink = "SELECT COUNT(*) FROM dataset_bucket" + whereClause;

  private static final String sqlGetSuccessfulIngestCount =
      "SELECT successful_ingests FROM dataset_bucket" + whereClause;

  private static final String sqlDeleteLink = "DELETE FROM dataset_bucket" + whereClause;

  private static final String sqlGetBucketResourceId =
      "SELECT bucket_resource_id FROM dataset_bucket WHERE dataset_id = :dataset_id";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public DatasetBucketDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public String getProjectResourceForBucket(UUID datasetId, UUID billingId)
      throws GoogleResourceException {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("profile_id", billingId);
    List<String> results =
        jdbcTemplate.query(sqlGetProjectIdForBucket, params, new DatasetBucketProjectMapper());
    // case where we need to create new google project
    if (results.size() == 0) {
      logger.info(
          "Google project does not exist for dataset {} and billing profile {}",
          datasetId,
          billingId);
      return null;
    } else if (results.size() > 1) {
      throw new GoogleResourceException(
          "There should only be one google project per dataset/billing combo.");
    }
    // case where we can re-use an exisiting google project
    return results.get(0);
  }

  private static class DatasetBucketProjectMapper implements RowMapper<String> {
    public String mapRow(ResultSet rs, int rowNum) throws SQLException {
      return rs.getObject("google_project_id", String.class);
    }
  }

  public List<UUID> getProjectResourceIdsForBucketPerDataset(UUID datasetId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", datasetId);
    return jdbcTemplate.query(
        sqlGetProjectResourceForBucketPerDataset, params, new DatasetBucketMapper());
  }

  private static class DatasetBucketMapper implements RowMapper<UUID> {
    public UUID mapRow(ResultSet rs, int rowNum) throws SQLException {
      return rs.getObject("project_resource_id", UUID.class);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createDatasetBucketLink(UUID datasetId, UUID bucketResourceId) {
    if (datasetBucketLinkExists(datasetId, bucketResourceId)) {
      // If the link is already made then increment our use of it.
      incrementDatasetBucketLink(datasetId, bucketResourceId);
    } else {
      // Not there, try creating it
      datasetBucketLinkUpdate(sqlCreateLink, datasetId, bucketResourceId);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteDatasetBucketLink(UUID datasetId, UUID bucketResourceId) {
    datasetBucketLinkUpdate(sqlDeleteLink, datasetId, bucketResourceId);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void decrementDatasetBucketLink(UUID datasetId, UUID bucketResourceId) {
    datasetBucketLinkUpdate(sqlDecrementCount, datasetId, bucketResourceId);
  }

  boolean datasetBucketLinkExists(UUID datasetId, UUID bucketResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("bucket_resource_id", bucketResourceId);
    Integer count = jdbcTemplate.queryForObject(sqlExistsLink, params, Integer.class);
    if (count == null) {
      throw new CorruptMetadataException("Impossible null value from count");
    }
    return (count == 1);
  }

  public List<UUID> getBucketResourceIdForDatasetId(UUID datasetId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("dataset_id", datasetId);
    return jdbcTemplate.query(sqlGetBucketResourceId, params, new UuidMapper("bucket_resource_id"));
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  int datasetBucketSuccessfulIngestCount(UUID datasetId, UUID bucketResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("bucket_resource_id", bucketResourceId);
    Integer count = jdbcTemplate.queryForObject(sqlGetSuccessfulIngestCount, params, Integer.class);
    if (count == null) {
      throw new CorruptMetadataException("Impossible null value from count");
    }
    return count;
  }

  private void incrementDatasetBucketLink(UUID datasetId, UUID bucketResourceId) {
    datasetBucketLinkUpdate(sqlIncrementCount, datasetId, bucketResourceId);
  }

  private void datasetBucketLinkUpdate(String sql, UUID datasetId, UUID bucketResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("dataset_id", datasetId)
            .addValue("bucket_resource_id", bucketResourceId);
    try {
      jdbcTemplate.update(sql, params);
    } catch (DataAccessException dataAccessException) {
      if (retryQuery(dataAccessException)) {
        logger.error("datasetBucket link operation failed with retryable exception.");
        throw new RetryQueryException("Retry", dataAccessException);
      }
      logger.error("datasetBucket link operation failed with fatal exception.");
      throw dataAccessException;
    }
  }

  private static class UuidMapper implements RowMapper<UUID> {
    private String columnLabel;

    UuidMapper(String columnLabel) {
      this.columnLabel = columnLabel;
    }

    public UUID mapRow(ResultSet rs, int rowNum) throws SQLException {
      return rs.getObject(this.columnLabel, UUID.class);
    }
  }
}
