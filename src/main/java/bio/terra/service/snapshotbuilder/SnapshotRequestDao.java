package bio.terra.service.snapshotbuilder;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class SnapshotRequestDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private static final String SOURCE_SNAPSHOT_ID = "source_snapshot_id";
  private static final String ID = "id";
  private static final String SNAPSHOT_NAME = "snapshot_name";
  private static final String SNAPSHOT_RESEARCH_PURPOSE = "snapshot_research_purpose";
  private static final String SNAPSHOT_SPECIFICATION = "snapshot_specification";
  private static final String CREATED_BY = "created_by";
  private static final String CREATED_DATE = "created_date";
  private static final String STATUS_UPDATED_DATE = "status_updated_date";
  private static final String STATUS = "status";
  private static final String FLIGHT_ID = "flightid";
  private static final String CREATED_SNAPSHOT_ID = "created_snapshot_id";
  private static final String SAM_GROUP_NAME = "sam_group_name";
  private static final String SAM_GROUP_EMAIL = "sam_group_email";
  private static final String SAM_GROUP_CREATED_BY = "sam_group_created_by";
  private static final String AUTHORIZED_RESOURCES = "authorized_resources";
  private static final String NOT_FOUND_MESSAGE =
      "Snapshot Access Request with given id does not exist.";

  private final RowMapper<SnapshotAccessRequestModel> modelMapper =
      (rs, rowNum) ->
          new SnapshotAccessRequestModel(
              rs.getObject(ID, UUID.class),
              rs.getString(SNAPSHOT_NAME),
              rs.getString(SNAPSHOT_RESEARCH_PURPOSE),
              rs.getObject(SOURCE_SNAPSHOT_ID, UUID.class),
              mapRequestFromJson(rs.getString(SNAPSHOT_SPECIFICATION)),
              rs.getString(CREATED_BY),
              DaoUtils.getInstant(rs, CREATED_DATE),
              DaoUtils.getInstant(rs, STATUS_UPDATED_DATE),
              SnapshotAccessRequestStatus.valueOf(rs.getString(STATUS)),
              rs.getObject(CREATED_SNAPSHOT_ID, UUID.class),
              rs.getString(FLIGHT_ID),
              rs.getString(SAM_GROUP_NAME),
              rs.getString(SAM_GROUP_EMAIL),
              rs.getString(SAM_GROUP_CREATED_BY));

  public SnapshotRequestDao(
      NamedParameterJdbcTemplate jdbcTemplate,
      @Qualifier("daoObjectMapper") ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  private SnapshotBuilderRequest mapRequestFromJson(String json) {
    try {
      return objectMapper.readValue(json, SnapshotBuilderRequest.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Could not read SnapshotBuilderRequest from json.");
    }
  }

  /**
   * Get the Snapshot Request associated with the given database id.
   *
   * @param requestId associated with one snapshot request.
   * @return the specified snapshot request or exception if it does not exist.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public SnapshotAccessRequestModel getById(UUID requestId) {
    String sql = "SELECT * FROM snapshot_request WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(ID, requestId);
    try {
      return jdbcTemplate.queryForObject(sql, params, modelMapper);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot access requests found for given id", ex);
    }
  }

  /**
   * Return the list of Snapshot Requests associated with the given snapshot id.
   *
   * @param authorizedResources snapshot requests that the user has permission to see.
   * @return the list of snapshot requests, empty if none, or an exception if the snapshot does not
   *     exist.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<SnapshotAccessRequestModel> enumerate(Collection<UUID> authorizedResources) {
    String sql =
        "SELECT * FROM snapshot_request WHERE id IN (:authorized_resources) AND status != :status";
    if (authorizedResources.isEmpty()) {
      return List.of();
    }
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(AUTHORIZED_RESOURCES, authorizedResources)
            .addValue(STATUS, SnapshotAccessRequestStatus.DELETED.toString());
    try {
      return jdbcTemplate.query(sql, params, modelMapper);
    } catch (EmptyResultDataAccessException ex) {
      return List.of();
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<SnapshotAccessRequestModel> enumerateBySnapshot(UUID snapshotId) {
    String sql =
        "SELECT * FROM snapshot_request WHERE source_snapshot_id = :source_snapshot_id AND status != :status";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(SOURCE_SNAPSHOT_ID, snapshotId)
            .addValue(STATUS, SnapshotAccessRequestStatus.DELETED.toString());
    try {
      return jdbcTemplate.query(sql, params, modelMapper);
    } catch (EmptyResultDataAccessException ex) {
      return List.of();
    }
  }

  /**
   * Create a new Snapshot Access Request for the given snapshot id.
   *
   * @param request the snapshot access request.
   * @param email the email of the user creating the request.
   * @return the created snapshot access request response.
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotAccessRequestModel create(SnapshotAccessRequest request, String email) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(request.getSnapshotBuilderRequest());
    } catch (JsonProcessingException e) {
      throw new BadRequestException("Could not write snapshot access request to json", e);
    }
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    String sql =
        """
        INSERT INTO snapshot_request
        (source_snapshot_id, snapshot_name, snapshot_research_purpose, snapshot_specification, created_by)
        VALUES (:source_snapshot_id, :snapshot_name, :snapshot_research_purpose, cast(:snapshot_specification as jsonb), :created_by)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(SOURCE_SNAPSHOT_ID, request.getSourceSnapshotId())
            .addValue(SNAPSHOT_NAME, request.getName())
            .addValue(SNAPSHOT_RESEARCH_PURPOSE, request.getResearchPurposeStatement())
            .addValue(SNAPSHOT_SPECIFICATION, jsonValue)
            .addValue(CREATED_BY, email);
    try {
      jdbcTemplate.update(sql, params, keyHolder);
    } catch (DataIntegrityViolationException ex) {
      throw new NotFoundException("Snapshot with given snapshot id does not exist.");
    }
    UUID id = keyHolder.getId();
    return getById(id);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateStatus(UUID requestId, SnapshotAccessRequestStatus status) {
    String sql =
        """
        UPDATE snapshot_request SET
        status = :status, status_updated_date = now()
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue(STATUS, status.toString()).addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException(NOT_FOUND_MESSAGE);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateFlightId(UUID requestId, String flightId) {
    String sql =
        """
        UPDATE snapshot_request SET
        flightid = :flightid
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue(FLIGHT_ID, flightId).addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException(NOT_FOUND_MESSAGE);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateCreatedSnapshotId(UUID requestId, UUID snapshotId) {
    String sql =
        """
        UPDATE snapshot_request SET
        created_snapshot_id = :created_snapshot_id
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(CREATED_SNAPSHOT_ID, snapshotId)
            .addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException(NOT_FOUND_MESSAGE);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void updateSamGroup(
      UUID requestId, String samGroupName, String samGroupEmail, String createdByEmail) {
    String sql =
        """
        UPDATE snapshot_request SET
        sam_group_name = :sam_group_name,
        sam_group_email = :sam_group_email,
        sam_group_created_by = :sam_group_created_by
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(SAM_GROUP_NAME, samGroupName)
            .addValue(SAM_GROUP_EMAIL, samGroupEmail)
            .addValue(SAM_GROUP_CREATED_BY, createdByEmail)
            .addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException(NOT_FOUND_MESSAGE);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void delete(UUID requestId) {
    String sql = "DELETE FROM snapshot_request WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException("Snapshot Request with given id does not exist.");
    }
  }
}
