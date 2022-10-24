package bio.terra.service.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired
  public DuosDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      @Qualifier("tdrServiceAccountEmail") String tdrServiceAccountEmail) {
    this.jdbcTemplate = jdbcTemplate;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  @VisibleForTesting
  String getTdrServiceAccountEmail() {
    return tdrServiceAccountEmail;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public DuosFirecloudGroupModel insertAndRetrieveFirecloudGroup(
      String duosId, String firecloudGroupName, String firecloudGroupEmail) {
    String sql =
        """
        INSERT INTO duos_firecloud_group
        (duos_id, firecloud_group_name, firecloud_group_email, created_by)
        VALUES (:duos_id, :firecloud_group_name, :firecloud_group_email, :created_by)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("duos_id", duosId)
            .addValue("firecloud_group_name", firecloudGroupName)
            .addValue("firecloud_group_email", firecloudGroupEmail)
            .addValue("created_by", tdrServiceAccountEmail);
    jdbcTemplate.update(sql, params);
    logger.info("Inserted {} -> {} into duos_firecloud_group", duosId, firecloudGroupName);
    return retrieveFirecloudGroup(duosId);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public DuosFirecloudGroupModel retrieveFirecloudGroup(String duosId) {
    try {
      String sql =
          """
          SELECT duos_id, firecloud_group_name, firecloud_group_email, created_by, created_date,
            last_synced_date
          FROM duos_firecloud_group
          WHERE duos_id = :duos_id
          """;
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("duos_id", duosId);
      return jdbcTemplate.queryForObject(sql, params, new DuosFirecloudGroupMapper());
    } catch (EmptyResultDataAccessException ex) {
      return null;
    }
  }

  private static class DuosFirecloudGroupMapper implements RowMapper<DuosFirecloudGroupModel> {
    public DuosFirecloudGroupModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new DuosFirecloudGroupModel()
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