package bio.terra.service.snapshotbuilder;

import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.model.SnapshotBuilderSettings;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class SnapshotBuilderSettingsDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String DATASET_ID_FIELD = "dataset_id";
  private static final String SNAPSHOT_ID_FIELD = "snapshot_id";
  private static final String SETTINGS_FIELD = "settings";

  private static final RowMapper<SnapshotBuilderSettings> MAPPER =
      (rs, rowNum) -> {
        try {
          return objectMapper.readValue(
              rs.getString(SETTINGS_FIELD), SnapshotBuilderSettings.class);
        } catch (JsonProcessingException e) {
          throw new InternalServerErrorException(
              "Settings not stored as properly formatted json", e);
        }
      };

  @Autowired
  public SnapshotBuilderSettingsDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public SnapshotBuilderSettings getByDatasetId(UUID datasetId) {
    try {

      return jdbcTemplate.queryForObject(
          "SELECT settings FROM snapshot_builder_settings WHERE dataset_id = :dataset_id",
          Map.of(DATASET_ID_FIELD, datasetId),
          MAPPER);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for dataset", ex);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public SnapshotBuilderSettings getBySnapshotId(UUID snapshotId) {
    try {

      return jdbcTemplate.queryForObject(
          "SELECT settings FROM snapshot_builder_settings WHERE snapshot_id = :snapshot_id",
          Map.of(SNAPSHOT_ID_FIELD, snapshotId),
          MAPPER);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for snapshot", ex);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotBuilderSettings upsertByDatasetId(
      UUID datasetId, SnapshotBuilderSettings settings) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(settings);
    } catch (JsonProcessingException e) {
      throw new BadRequestException("Could not write settings to json", e);
    }
    jdbcTemplate.update(
        "INSERT INTO snapshot_builder_settings (dataset_id, settings)"
            + " VALUES (:dataset_id, cast(:settings as jsonb))"
            + " ON CONFLICT ON CONSTRAINT snapshot_builder_settings_dataset_id_key"
            + " DO UPDATE SET settings = cast(:settings as jsonb)",
        Map.of(DATASET_ID_FIELD, datasetId, SETTINGS_FIELD, jsonValue));
    return getByDatasetId(datasetId);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotBuilderSettings upsertBySnapshotId(
      UUID snapshotId, SnapshotBuilderSettings settings) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(settings);
    } catch (JsonProcessingException e) {
      throw new BadRequestException("Could not write settings to json", e);
    }
    jdbcTemplate.update(
        "INSERT INTO snapshot_builder_settings (snapshot_id, settings)"
            + " VALUES (:snapshot_id, cast(:settings as jsonb))"
            + " ON CONFLICT ON CONSTRAINT snapshot_builder_settings_snapshot_id_key"
            + " DO UPDATE SET settings = cast(:settings as jsonb)",
        Map.of(SNAPSHOT_ID_FIELD, snapshotId, SETTINGS_FIELD, jsonValue));
    return getBySnapshotId(snapshotId);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteByDatasetId(UUID datasetId) {
    try {
      jdbcTemplate.update(
          "DELETE FROM snapshot_builder_settings WHERE dataset_id = :dataset_id",
          Map.of(DATASET_ID_FIELD, datasetId));
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for dataset", ex);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteBySnapshotId(UUID snapshotId) {
    try {
      jdbcTemplate.update(
          "DELETE FROM snapshot_builder_settings WHERE snapshot_id = :snapshot_id",
          Map.of(SNAPSHOT_ID_FIELD, snapshotId));
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for snapshot", ex);
    }
  }
}
