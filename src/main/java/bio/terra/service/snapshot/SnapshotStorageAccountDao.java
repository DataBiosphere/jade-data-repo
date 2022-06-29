package bio.terra.service.snapshot;

import static bio.terra.common.DaoUtils.retryQuery;

import bio.terra.common.DaoUtils.UuidMapper;
import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

@Component
public class SnapshotStorageAccountDao {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotStorageAccountDao.class);

  private static final String whereClause = " WHERE id = :snapshot_id";

  private static final String sqlCreateLink =
      "UPDATE snapshot "
          + " SET storage_account_resource_id = :storage_account_resource_id"
          + whereClause;

  private static final String sqlGetStorageAccountResourceId =
      "SELECT storage_account_resource_id FROM snapshot"
          + whereClause
          + " AND storage_account_resource_id IS NOT NULL";

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
      int update = jdbcTemplate.update(sqlCreateLink, params);

      if (update == 0) {
        throw new CorruptMetadataException("Could not link storage account to snapshot");
      }
    } catch (DataAccessException dataAccessException) {
      if (retryQuery(dataAccessException)) {
        logger.error("snapshotStorageAccount link operation failed with retryable exception.");
        throw new RetryQueryException("Retry", dataAccessException);
      }
      logger.error("snapshotStorageAccount link operation failed with fatal exception.");
      throw dataAccessException;
    }
  }

  public Optional<UUID> getStorageAccountResourceIdForSnapshotId(UUID snapshotId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("snapshot_id", snapshotId);

    List<UUID> storageAccountResourceIds =
        jdbcTemplate.query(
            sqlGetStorageAccountResourceId, params, new UuidMapper("storage_account_resource_id"));

    if (!storageAccountResourceIds.isEmpty()) {
      return Optional.of(storageAccountResourceIds.get(0));
    } else {
      return Optional.empty();
    }
  }
}
