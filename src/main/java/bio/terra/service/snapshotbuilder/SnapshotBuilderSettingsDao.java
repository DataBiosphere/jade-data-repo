package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderSettings;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ServerErrorException;
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
public class SnapshotBuilderSettingsDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private static ObjectMapper objectMapper = new ObjectMapper();

  private static class SnapshotBuilderSettingsMapper implements RowMapper<SnapshotBuilderSettings> {

    public SnapshotBuilderSettings mapRow(ResultSet rs, int rowNum) throws SQLException {

      try {
        return objectMapper.readValue(rs.getString("settings"), SnapshotBuilderSettings.class);
      } catch (JsonProcessingException e) {
        throw new ServerErrorException("Settings not stored as properly formatted json", 500, e);
      }
    }
  }

  @Autowired
  public SnapshotBuilderSettingsDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public SnapshotBuilderSettings getSnapshotBuilderSettingsByDatasetId(UUID id) {
    try {

      return jdbcTemplate.queryForObject(
          "SELECT settings FROM snapshot_builder_settings WHERE dataset_id = :id",
          new MapSqlParameterSource().addValue("id", id),
          new SnapshotBuilderSettingsMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for dataset", ex);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotBuilderSettings upsertSnapshotBuilderSettingsByDataset(
      UUID id, SnapshotBuilderSettings settings) {
    Gson gson = new Gson();
    MapSqlParameterSource mapSqlParameterSource =
        new MapSqlParameterSource().addValue("id", id).addValue("settings", gson.toJson(settings));
    try {
      getSnapshotBuilderSettingsByDatasetId(id);
      jdbcTemplate.update(
          "UPDATE snapshot_builder_settings SET settings = :settings WHERE dataset_id = :id",
          mapSqlParameterSource);
    } catch (NotFoundException ex) {
      jdbcTemplate.update(
          "INSERT INTO snapshot_builder_settings (dataset_id, settings) VALUES (:id, :settings)",
          mapSqlParameterSource);
    }
    return getSnapshotBuilderSettingsByDatasetId(id);
  }
}
