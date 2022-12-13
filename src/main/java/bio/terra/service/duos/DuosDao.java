package bio.terra.service.duos;

import bio.terra.common.DaoKeyHolder;
import bio.terra.model.DuosFirecloudGroupModel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DuosDao {
  private static final Logger logger = LoggerFactory.getLogger(DuosDao.class);

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final String tdrServiceAccountEmail;

  private static final String DUOS_FIRECLOUD_GROUP_QUERY =
      """
      SELECT id, duos_id, firecloud_group_name, firecloud_group_email, created_by, created_date,
        last_synced_date
      FROM duos_firecloud_group
      """;
  private static final DuosFirecloudGroupMapper DUOS_FIRECLOUD_GROUP_MAPPER =
      new DuosFirecloudGroupMapper();

  public DuosDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      @Qualifier("tdrServiceAccountEmail") String tdrServiceAccountEmail) {
    this.jdbcTemplate = jdbcTemplate;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public DuosFirecloudGroupModel insertAndRetrieveFirecloudGroup(DuosFirecloudGroupModel created) {
    UUID id = insertFirecloudGroup(created);
    return retrieveFirecloudGroup(id);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public UUID insertFirecloudGroup(DuosFirecloudGroupModel created) {
    String sql =
        """
        INSERT INTO duos_firecloud_group
        (duos_id, firecloud_group_name, firecloud_group_email, created_by)
        VALUES (:duos_id, :firecloud_group_name, :firecloud_group_email, :created_by)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("duos_id", created.getDuosId())
            .addValue("firecloud_group_name", created.getFirecloudGroupName())
            .addValue("firecloud_group_email", created.getFirecloudGroupEmail())
            .addValue("created_by", tdrServiceAccountEmail);
    DaoKeyHolder keyHolder = new DaoKeyHolder();

    jdbcTemplate.update(sql, params, keyHolder);
    logger.info(
        "Inserted {} -> {} into duos_firecloud_group",
        created.getDuosId(),
        created.getFirecloudGroupName());
    return keyHolder.getId();
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<DuosFirecloudGroupModel> retrieveFirecloudGroups() {
    return jdbcTemplate.query(DUOS_FIRECLOUD_GROUP_QUERY, DUOS_FIRECLOUD_GROUP_MAPPER);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public DuosFirecloudGroupModel retrieveFirecloudGroup(UUID id) {
    try {
      String sql = DUOS_FIRECLOUD_GROUP_QUERY + " WHERE id = :id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(sql, params, DUOS_FIRECLOUD_GROUP_MAPPER);
    } catch (EmptyResultDataAccessException ex) {
      return null;
    }
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public DuosFirecloudGroupModel retrieveFirecloudGroupByDuosId(String duosId) {
    try {
      String sql = DUOS_FIRECLOUD_GROUP_QUERY + " WHERE duos_id = :duos_id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("duos_id", duosId);
      return jdbcTemplate.queryForObject(sql, params, DUOS_FIRECLOUD_GROUP_MAPPER);
    } catch (EmptyResultDataAccessException ex) {
      return null;
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean updateFirecloudGroupLastSyncedDate(UUID id, Instant lastSyncedDate) {
    logger.info("Updating Firecloud group record {} last synced date to {}", id, lastSyncedDate);
    String sql =
        """
            UPDATE duos_firecloud_group
            SET last_synced_date = :last_synced_date
            WHERE id = :id
            """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("last_synced_date", Timestamp.from(lastSyncedDate));
    int rowsAffected = jdbcTemplate.update(sql, params);
    return rowsAffected > 0;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean deleteFirecloudGroup(UUID id) {
    logger.info("Deleting Firecloud group record {}", id);
    String sql = "DELETE FROM duos_firecloud_group WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    int rowsAffected = jdbcTemplate.update(sql, params);
    return rowsAffected > 0;
  }

  private static class DuosFirecloudGroupMapper implements RowMapper<DuosFirecloudGroupModel> {
    public DuosFirecloudGroupModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new DuosFirecloudGroupModel()
          .id(rs.getObject("id", UUID.class))
          .duosId(rs.getString("duos_id"))
          .firecloudGroupName(rs.getString("firecloud_group_name"))
          .firecloudGroupEmail(rs.getString("firecloud_group_email"))
          .createdBy(rs.getString("created_by"))
          .created(getInstantString(rs, "created_date"))
          .lastSynced(getInstantString(rs, "last_synced_date"));
    }

    private String getInstantString(ResultSet rs, String columnLabel) throws SQLException {
      Timestamp timestamp = rs.getTimestamp(columnLabel);
      if (timestamp != null) {
        return timestamp.toInstant().toString();
      }
      return null;
    }
  }
}
