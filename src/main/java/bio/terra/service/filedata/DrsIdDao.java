package bio.terra.service.filedata;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DrsIdDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;

  public DrsIdDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public static final String INSERT_DRS_ID =
      """
      INSERT INTO drs_id (drs_object_id, snapshot_id) VALUES (:drs_object_id, :snapshot_id)
      ON CONFLICT DO NOTHING
      """;
  public static final String DELETE_DRS_ID_BY_SNAPSHOT =
      """
      DELETE FROM drs_id WHERE snapshot_id = :snapshot_id
      """;

  public static final String ENUMERATE_DRS_IDS_BY_DRS_ID =
      """
      SELECT id, drs_object_id, snapshot_id FROM drs_id WHERE drs_object_id = :drs_object_id
      """;

  public long recordDrsIdToSnapshot(UUID snapshotId, List<DrsId> drsIds) {
    MapSqlParameterSource[] parameters =
        drsIds.stream()
            .map(
                i ->
                    new MapSqlParameterSource()
                        .addValue("drs_object_id", i.toDrsObjectId())
                        .addValue("snapshot_id", snapshotId))
            .toArray(MapSqlParameterSource[]::new);
    int[] affectedRows = jdbcTemplate.batchUpdate(INSERT_DRS_ID, parameters);
    return Arrays.stream(affectedRows).reduce(0, Integer::sum);
  }

  public long deleteDrsIdToSnapshotsBySnapshot(UUID snapshotId) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("snapshot_id", snapshotId);
    return jdbcTemplate.update(DELETE_DRS_ID_BY_SNAPSHOT, parameters);
  }

  public List<UUID> retrieveReferencedSnapshotIds(DrsId drsId) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("drs_object_id", drsId.toDrsObjectId());
    return jdbcTemplate.query(
        ENUMERATE_DRS_IDS_BY_DRS_ID,
        parameters,
        (rs, rowNum) -> UUID.fromString(rs.getString("snapshot_id")));
  }
}
