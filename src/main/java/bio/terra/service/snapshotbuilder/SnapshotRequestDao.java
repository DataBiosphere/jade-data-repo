package bio.terra.service.snapshotbuilder;

import static bio.terra.common.DaoUtils.getInstantString;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;
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
  private static final String DATASET_ID = "dataset_id";
  private static final String SNAPSHOT_NAME = "snapshot_name";
  private static final String SNAPSHOT_RESEARCH_PURPOSE = "snapshot_research_purpose";
  private static final String SNAPSHOT_SPECIFICATION = "snapshot_specification";
  private static final String CREATED_BY = "created_by";
  private static final String CREATED_DATE = "created_date";
  private static final String UPDATED_DATE = "updated_date";
  private static final String STATUS = "status";
  private static final String AUTHORIZED_RESOURCES = "authorized_resources";

  private final RowMapper<SnapshotAccessRequestResponse> responseMapper =
      (rs, rowNum) ->
          new SnapshotAccessRequestResponse()
              .id(rs.getObject(ID, UUID.class))
              .datasetId(rs.getObject(DATASET_ID, UUID.class))
              .sourceSnapshotId(rs.getObject(SOURCE_SNAPSHOT_ID, UUID.class))
              .snapshotName(rs.getString(SNAPSHOT_NAME))
              .snapshotResearchPurpose(rs.getString(SNAPSHOT_RESEARCH_PURPOSE))
              .snapshotSpecification(mapRequestFromJson(rs.getString(SNAPSHOT_SPECIFICATION)))
              .createdDate(getInstantString(rs, CREATED_DATE))
              .updatedDate(getInstantString(rs, UPDATED_DATE))
              .createdBy(rs.getString(CREATED_BY))
              .status(SnapshotAccessRequestStatus.valueOf(rs.getString(STATUS)));

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
  public SnapshotAccessRequestResponse getById(UUID requestId) {
    String sql = "SELECT * FROM snapshot_request WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(ID, requestId);
    try {
      return jdbcTemplate.queryForObject(sql, params, responseMapper);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot access requests found for given id", ex);
    }
  }

  /**
   * Return the list of Snapshot Requests associated with the given dataset id.
   *
   * @param authorizedResources snapshot requests that the user has permission to see.
   * @return the list of snapshot requests, empty if none, or an exception if the dataset does not
   *     exist.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<SnapshotAccessRequestResponse> enumerate(Set<UUID> authorizedResources) {
    String sql = "SELECT * FROM snapshot_request WHERE id IN(:authorized_resources)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(
                AUTHORIZED_RESOURCES,
                String.join(
                    ",",
                    authorizedResources.size() > 0
                        ? authorizedResources.stream().map(UUID::toString).toList()
                        : List.of("NULL")));
    try {
      return jdbcTemplate.query(sql, params, responseMapper);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot requests found for user", ex);
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
  public SnapshotAccessRequestResponse create(SnapshotAccessRequest request, String email) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(request.getDatasetRequest());
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
            .addValue(SOURCE_SNAPSHOT_ID, request.getId())
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
  public SnapshotAccessRequestResponse update(UUID requestId, SnapshotAccessRequestStatus status) {
    String sql =
        """
        UPDATE snapshot_request SET
        status = :status, updated_date = :updated_date
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(STATUS, status.toString())
            .addValue(UPDATED_DATE, Timestamp.from(Instant.now()))
            .addValue(ID, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException("Snapshot Access Request with given id does not exist.");
    }
    return getById(requestId);
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
