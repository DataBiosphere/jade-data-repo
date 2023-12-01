package bio.terra.service.snapshotbuilder;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
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
public class SnapshotRequestDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String requestIdField = "request_id";
  private static final String datasetIdField = "dataset_id";
  private static final String requestNameField = "request_name";
  private static final String requestResearchPurposeField = "request_research_purpose";
  private static final String snapshotBuilderRequestField = "snapshot_builder_request";
  private static final String userEmailField = "user_email";

  @Autowired
  public SnapshotRequestDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * Get the Snapshot Request associated with the given database id.
   *
   * @param requestId associated with one snapshot request.
   * @return the specified snapshot request or exception if it does not exist.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public SnapshotAccessRequestResponse getById(UUID requestId) {
    try {

      return jdbcTemplate.queryForObject(
          "SELECT * FROM snapshot_requests WHERE request_id = :request_id",
          Map.of(requestIdField, requestId),
          SnapshotAccessRequestResponse.class);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for dataset", ex);
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
  public List<SnapshotAccessRequestResponse> enumerateByDatasetId(
      UUID datasetId) {
    try {

      return jdbcTemplate.queryForList(
          "SELECT * FROM snapshot_requests WHERE dataset_id = :dataset_id",
          Map.of(datasetIdField, datasetId),
          SnapshotAccessRequestResponse.class);
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot builder settings found for dataset", ex);
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
        (dataset_id, request_name, request_research_purpose, snapshot_builder_request, request, user_email)
        VALUES (:dataset_id, :request_name, request_research_purpose, cast(:snapshot_builder_request as jsonb), :user_email)
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(datasetIdField, datasetId)
            .addValue(requestNameField, request.getName())
            .addValue(requestResearchPurposeField, request.getResearchPurposeStatement())
            .addValue(snapshotBuilderRequestField, jsonValue)
            .addValue(userEmailField, email);
    jdbcTemplate.update(sql,params, keyHolder);
    UUID id = keyHolder.getId();
    return getById(id);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void delete(UUID requestId) {
    try {
      jdbcTemplate.update(
          "DELETE FROM snapshot_requests WHERE request_id = :request_id",
          Map.of(requestIdField, requestId));
    } catch (EmptyResultDataAccessException ex) {
      throw new NotFoundException("No snapshot request found for given id", ex);
    }
  }
}
