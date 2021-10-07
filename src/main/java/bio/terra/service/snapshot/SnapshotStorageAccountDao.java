package bio.terra.service.snapshot;

import bio.terra.common.exception.RetryQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static bio.terra.common.DaoUtils.retryQuery;

@Component
public class SnapshotStorageAccountDao {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotStorageAccountDao.class);

  private static final String whereClause =
      " WHERE id = :snapshot_id";

  private static final String sqlCreateLink =
      "UPDATE snapshot "
          + " SET storage_account_resource_id = :storage_account_resource_id"
          + whereClause;

  private static final String sqlExistsLink =
      "SELECT storage_account_resource_id FROM snapshot" + whereClause;

  private static final String sqlGetStorageAccountResourceId =
      "SELECT storage_account_resource_id FROM snapshot" +
      whereClause;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SnapshotStorageAccountDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void createSnapshotStorageAccountLink(UUID snapshotId, UUID storageAccountResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("snapshot_id", snapshotId)
            .addValue("storage_account_resource_id", storageAccountResourceId);

    try {
      jdbcTemplate.update(sqlCreateLink, params);
    } catch (DataAccessException dataAccessException) {
      if (retryQuery(dataAccessException)) {
        logger.error("snapshotStorageAccount link operation failed with retryable exception.");
        throw new RetryQueryException("Retry", dataAccessException);
      }
      logger.error("snapshotStorageAccount link operation failed with fatal exception.");
      throw dataAccessException;
    }
  }

  boolean snapshotStorageAccountLinkExists(UUID snapshotId, UUID storageAccountResourceId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("snapshot_id", snapshotId)
            .addValue("storage_account_resource_id", storageAccountResourceId);
    String id = jdbcTemplate.queryForObject(sqlExistsLink, params, String.class);
    return id != null;
  }

  public UUID getStorageAccountResourceIdForSnapshotId(UUID snapshotId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("snapshot_id", snapshotId);
    return jdbcTemplate.queryForObject(sqlGetStorageAccountResourceId, params, UUID.class);
  }

}
