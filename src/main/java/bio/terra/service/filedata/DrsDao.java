package bio.terra.service.filedata;

import bio.terra.common.iam.AuthenticatedUserRequest;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DrsDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;

  private final DrsIdService drsIdService;

  @Autowired
  public DrsDao(NamedParameterJdbcTemplate jdbcTemplate, DrsIdService drsIdService) {
    this.jdbcTemplate = jdbcTemplate;
    this.drsIdService = drsIdService;
  }

  public static final String INSERT_DRS_ID =
      """
      INSERT INTO drs_id (drs_object_id, snapshot_id) VALUES (:drs_object_id, :snapshot_id)
      ON CONFLICT DO NOTHING
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

  public static final String DELETE_DRS_ID_BY_SNAPSHOT =
      """
      DELETE FROM drs_id WHERE snapshot_id = :snapshot_id
      """;

  public long deleteDrsIdToSnapshotsBySnapshot(UUID snapshotId) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("snapshot_id", snapshotId);
    return jdbcTemplate.update(DELETE_DRS_ID_BY_SNAPSHOT, parameters);
  }

  public static final String ENUMERATE_DRS_IDS_BY_DRS_ID =
      """
      SELECT id, drs_object_id, snapshot_id FROM drs_id WHERE drs_object_id = :drs_object_id
      """;

  public List<UUID> retrieveReferencedSnapshotIds(DrsId drsId) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("drs_object_id", drsId.toDrsObjectId());
    return jdbcTemplate.query(
        ENUMERATE_DRS_IDS_BY_DRS_ID,
        parameters,
        (rs, rowNum) -> UUID.fromString(rs.getString("snapshot_id")));
  }

  public static final String INSERT_DRS_ALIAS =
      """
      INSERT INTO drs_alias (alias_drs_object_id, tdr_drs_object_id, created_by, flightid)
      VALUES (:alias_drs_object_id, :tdr_drs_object_id, :created_by, :flightid)
      """;

  /**
   * Register a new DRS alias.
   *
   * @param drsAliasSpecs The list of aliases to register
   * @param flightId The flight running the alias registration
   * @param userReq The user who triggered the registration
   * @return The number of rows inserted
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public long insertDrsAlias(
      List<DrsAliasSpec> drsAliasSpecs, String flightId, AuthenticatedUserRequest userReq) {
    MapSqlParameterSource[] parameters =
        drsAliasSpecs.stream()
            .map(
                a ->
                    new MapSqlParameterSource()
                        .addValue("alias_drs_object_id", a.aliasDrsObjectId())
                        .addValue("tdr_drs_object_id", a.tdrDrsObjectId.toDrsObjectId())
                        .addValue("created_by", userReq.getEmail())
                        .addValue("flightid", flightId))
            .toArray(MapSqlParameterSource[]::new);

    return Arrays.stream(jdbcTemplate.batchUpdate(INSERT_DRS_ALIAS, parameters))
        .reduce(0, Integer::sum);
  }

  public static final String GET_DRS_ALIAS_BY_ALIAS =
      """
      SELECT id, alias_drs_object_id, tdr_drs_object_id, created_date, created_by, flightid
      FROM drs_alias
      WHERE alias_drs_object_id = :alias_drs_object_id
      """;

  /**
   * Retrieve a DRS alias object by its alias ID.
   *
   * @param aliasDrsId The alias to use to look up the alias
   * @return The DRS alias object or null if not found
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public DrsAlias retrieveDrsAliasByAlias(String aliasDrsId) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("alias_drs_object_id", aliasDrsId);
    try {
      return jdbcTemplate.queryForObject(
          GET_DRS_ALIAS_BY_ALIAS,
          parameters,
          (rs, rowNum) ->
              new DrsAlias(
                  UUID.fromString(rs.getString("id")),
                  rs.getString("alias_drs_object_id"),
                  drsIdService.fromObjectId(rs.getString("tdr_drs_object_id")),
                  rs.getTimestamp("created_date").toInstant(),
                  rs.getString("created_by"),
                  rs.getString("flightid")));
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public static final String DELETE_DRS_ALIAS_BY_FLIGHT =
      """
      DELETE
      FROM drs_alias
      WHERE flightid = :flightid
      """;

  /**
   * Delete all DRS aliases created by a flight. Useful for cleaning up after a failed flight.
   *
   * @param flightId The flight running alias registration
   * @return the number of rows deleted
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public long deleteDrsAliasByFlight(String flightId) {
    MapSqlParameterSource parameters = new MapSqlParameterSource().addValue("flightid", flightId);
    return jdbcTemplate.update(DELETE_DRS_ALIAS_BY_FLIGHT, parameters);
  }

  public record DrsAliasSpec(String aliasDrsObjectId, DrsId tdrDrsObjectId) {}

  public record DrsAlias(
      UUID id,
      String aliasDrsObjectId,
      DrsId tdrDrsObjectId,
      Instant createdDate,
      String createdBy,
      String flightId) {}
}
