package bio.terra.service.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired
  public DuosDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public Optional<DuosFirecloudGroupModel> insertFirecloudGroupAndRetrieve(
      String duosId, String firecloudGroupName, String firecloudGroupEmail) {
    String sql =
        """
        INSERT INTO duos_firecloud_group
        (duos_id, firecloud_group_name, firecloud_group_email) VALUES
        (:duos_id, :firecloud_group_name, :firecloud_group_email)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("duos_id", duosId)
            .addValue("firecloud_group_name", firecloudGroupName)
            .addValue("firecloud_group_email", firecloudGroupEmail);
    int rowsAffected = jdbcTemplate.update(sql, params);
    boolean insertSucceeded = (rowsAffected == 1);

    if (insertSucceeded) {
      logger.info("Inserted mapping {} -> {}", duosId, firecloudGroupName);
    }
    return retrieveDuosFirecloudGroup(duosId);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean updateLastSyncedDate(String duosId, Instant lastSyncedDate) {
    String sql =
        """
        UPDATE duos_firecloud_group
        SET last_synced_date = :last_synced_date
        WHERE duos_id = :duos_id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("last_synced_date", Timestamp.from(lastSyncedDate))
            .addValue("duos_id", duosId);
    int rowsAffected = jdbcTemplate.update(sql, params);
    boolean updateSucceeded = (rowsAffected == 1);

    if (updateSucceeded) {
      logger.info("Updated last synced date for {} -> {}", duosId, lastSyncedDate);
    }
    return updateSucceeded;
  }

  public List<DuosFirecloudGroupModel> retrieveDuosFirecloudGroups() {
    String sql = "SELECT * FROM duos_firecloud_group";
    return jdbcTemplate.query(sql, new MapSqlParameterSource(), new DuosFirecloudGroupMapper());
  }

  public Optional<DuosFirecloudGroupModel> retrieveDuosFirecloudGroup(String duosId) {
    try {
      String sql = "SELECT * FROM duos_firecloud_group WHERE duos_id = :duos_id";
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("duos_id", duosId);
      return Optional.of(jdbcTemplate.queryForObject(sql, params, new DuosFirecloudGroupMapper()));
    } catch (EmptyResultDataAccessException ex) {
      return Optional.empty();
    }
  }

  private static class DuosFirecloudGroupMapper implements RowMapper<DuosFirecloudGroupModel> {
    public DuosFirecloudGroupModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new DuosFirecloudGroupModel()
          .duosId(rs.getString("duos_id"))
          .firecloudGroupName(rs.getString("firecloud_group_name"))
          .firecloudGroupEmail(rs.getString("firecloud_group_email"))
          .created(rs.getTimestamp("created_date").toInstant().toString())
          .lastSynced(rs.getTimestamp("last_synced_date").toInstant().toString());
    }
  }
}
