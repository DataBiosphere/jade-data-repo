package bio.terra.service.search;

import bio.terra.service.snapshot.Snapshot;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class SnapshotSearchMetadataDao {

  private static final String PUT_METADATA =
      "INSERT INTO snapshot_search_metadata(snapshot_id, metadata) " +
          "VALUES(:snapshot_id, :metadata) " +
          "ON CONFLICT ON CONSTRAINT unique_snapshot_metadata_id " +
          "DO UPDATE SET metadata = :metadata";

  private static final String DELETE_METADATA =
      "DELETE FROM snapshot_search_metadata where snapshot_id = :snapshot_id";

  private static final String GET_METADATA =
      "SELECT (snapshot_id, metadata) FROM snapshot_search_metadata " +
          "WHERE snapshot_id IN (:snapshot_ids)";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SnapshotSearchMetadataDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void putMetadata(Snapshot snapshot, String jsonData) {
    var params = new MapSqlParameterSource()
        .addValue("snapshot_id", snapshot.getId())
        .addValue("metadata", jsonData);
    jdbcTemplate.update(PUT_METADATA, params);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteMetadata(Snapshot snapshot) {
    var params = new MapSqlParameterSource()
        .addValue("snapshot_id", snapshot.getId());
    jdbcTemplate.update(DELETE_METADATA, params);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public Map<UUID, String> getMetadata(List<UUID> snapshotIds) {
    var params = new MapSqlParameterSource()
        .addValue("snapshot_ids", snapshotIds);
    Map<UUID, String> result = new HashMap<>();
    jdbcTemplate.query(GET_METADATA, params, rs -> {
        result.put(UUID.fromString(rs.getString(0)), rs.getString(1));
    });
    return result;
  }
}
