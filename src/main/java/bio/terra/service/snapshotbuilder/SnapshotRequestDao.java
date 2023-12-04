package bio.terra.service.snapshotbuilder;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
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
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SnapshotAccessRequestResponseMapper responseMapper =
      new SnapshotAccessRequestResponseMapper();
  private static final String requestIdField = "id";
  private static final String datasetIdField = "dataset_id";
  private static final String requestNameField = "request_name";
  private static final String requestResearchPurposeField = "request_research_purpose";
  private static final String snapshotBuilderRequestField = "snapshot_builder_request";
  private static final String userEmailField = "user_email";
  private static final String createDateField = "create_date";
  private static final String updateDateField = "update_date";
  private static final String statusField = "status";

  @Autowired
  public SnapshotRequestDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static class SnapshotAccessRequestResponseMapper
      implements RowMapper<SnapshotAccessRequestResponse> {
    public SnapshotAccessRequestResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new SnapshotAccessRequestResponse()
          .id(rs.getObject(requestIdField, UUID.class))
          .datasetId(rs.getObject(datasetIdField, UUID.class))
          .requestName(rs.getString(requestNameField))
          .requestResearchPurpose(rs.getString(requestResearchPurposeField))
          .request(mapRequestFromJson(rs.getString(snapshotBuilderRequestField)))
          .createDate(getInstantString(rs, createDateField))
          .updateDate(getInstantString(rs, updateDateField))
          .userEmail(rs.getString(userEmailField))
          .status(SnapshotAccessRequestResponse.StatusEnum.valueOf(rs.getString(statusField)));
    }

    private String getInstantString(ResultSet rs, String columnLabel) throws SQLException {
      Timestamp timestamp = rs.getTimestamp(columnLabel);
      if (timestamp != null) {
        return timestamp.toInstant().toString();
      }
      return null;
    }

    private static SnapshotBuilderRequest mapRequestFromJson(String json) {
      if (Objects.nonNull(json)) {
        try {
          return objectMapper.readValue(json, SnapshotBuilderRequest.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException("Could not read SnapshotBuilderRequest from json.");
        }
      } else {
        return new SnapshotBuilderRequest();
      }
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
    String sql = "SELECT * FROM snapshot_requests WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(requestIdField, requestId);
    try {

      return jdbcTemplate.queryForObject(sql, params, responseMapper);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot access requests found for given id", ex);
    }
  }

  /**
   * Return the list of Snapshot Requests associated with the given dataset id.
   *
   * @param datasetId associated with any number of snapshot requests.
   * @return the list of snapshot requests, empty if none, or an exception if the dataset does not
   *     exist.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<SnapshotAccessRequestResponse> enumerateByDatasetId(UUID datasetId) {
    String sql = "SELECT * FROM snapshot_requests WHERE dataset_id = :dataset_id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(datasetIdField, datasetId);
    try {
      return jdbcTemplate.query(sql, params, responseMapper);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No dataset found for given id", ex);
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotAccessRequestResponse create(
      UUID datasetId, SnapshotAccessRequest request, String email) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(request.getDatasetRequest());
    } catch (JsonProcessingException e) {
      throw new BadRequestException("Could not write snapshot access request to json", e);
    }
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    String sql =
        """
        INSERT INTO snapshot_requests
        (dataset_id, request_name, request_research_purpose, snapshot_builder_request, user_email)
        VALUES (:dataset_id, :request_name, :request_research_purpose, cast(:snapshot_builder_request as jsonb), :user_email)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(datasetIdField, datasetId)
            .addValue(requestNameField, request.getName())
            .addValue(requestResearchPurposeField, request.getResearchPurposeStatement())
            .addValue(snapshotBuilderRequestField, jsonValue)
            .addValue(userEmailField, email);
    try {
      jdbcTemplate.update(sql, params, keyHolder);
    } catch (DataIntegrityViolationException ex) {
      throw new NotFoundException("Dataset with given dataset id does not exist.");
    }
    UUID id = keyHolder.getId();
    return getById(id);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public SnapshotAccessRequestResponse update(
      UUID requestId, SnapshotAccessRequestResponse.StatusEnum status) {
    String sql =
        """
        UPDATE snapshot_requests SET
        status = :status, update_date = :update_date
        WHERE id = :id
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(statusField, status.toString())
            .addValue(updateDateField, Timestamp.from(Instant.now()))
            .addValue(requestIdField, requestId);
    if (jdbcTemplate.update(sql, params) == 0) {
      throw new NotFoundException("Snapshot Access Request with given id does not exist.");
    }
    return getById(requestId);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void delete(UUID requestId) {
    String sql = "DELETE FROM snapshot_requests WHERE id = :id";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue(requestIdField, requestId);
    try {
      if (jdbcTemplate.update(sql, params) == 0) {
        throw new NotFoundException("Snapshot Request with given id does not exist.");
      }
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot request found for given id", ex);
    }
  }
}
