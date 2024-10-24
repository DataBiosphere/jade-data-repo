package bio.terra.service.journal;

import bio.terra.common.DaoKeyHolder;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JournalDao {

  public static final String TABLE_NAME = "journal";
  private static final Logger logger = LoggerFactory.getLogger(JournalDao.class);
  private final NamedParameterJdbcTemplate jdbcTemplate;

  private final ObjectMapper objectMapper;

  private static final String summaryQueryColumns =
      "id, entry_type, user_email, resource_key, resource_type, "
          + "class_name, method_name, mutation, note, entry_timestamp ";

  @Autowired
  public JournalDao(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public UUID create(
      @NotNull JournalService.EntryType entryType,
      @NotNull String user,
      @NotNull UUID resource_key,
      @NotNull IamResourceType resourceType,
      @NotNull String className,
      @NotNull String methodName,
      String note,
      String mutationAsJson) {
    String sql =
        "INSERT INTO "
            + TABLE_NAME
            + " "
            + "(entry_type, user_email, resource_key, resource_type, class_name, method_name, "
            + "mutation, note, entry_timestamp) "
            + "VALUES(:entryType, :user, :resource_key, :resourceType, :className, :methodName, "
            + "cast(:mutation as jsonb), :note, current_timestamp)";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("entryType", entryType.toString())
            .addValue("user", user)
            .addValue("resource_key", resource_key)
            .addValue("resourceType", resourceType.toString())
            .addValue("className", className)
            .addValue("methodName", methodName)
            .addValue("mutation", mutationAsJson)
            .addValue("note", note);
    DaoKeyHolder journalKey = new DaoKeyHolder();
    int resultCount = jdbcTemplate.update(sql, params, journalKey);
    if (resultCount < 1) {
      logger.error(
          "Error writing journal entry {} {} {} {} {} {} {}",
          entryType,
          user,
          resource_key,
          resourceType,
          className,
          methodName,
          mutationAsJson);
    }
    return journalKey.getId();
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteJournalEntries(
      @NotNull UUID resource_key, @NotNull IamResourceType resourceType) {
    String sql =
        "DELETE FROM "
            + TABLE_NAME
            + " WHERE resource_key = :resource_key AND resource_type = :resourceType";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("resource_key", resource_key)
            .addValue("resourceType", resourceType.toString());

    int entriesRemoved = jdbcTemplate.update(sql, params);
    logger.warn("{} journal entries removed for {} {}", entriesRemoved, resource_key, resourceType);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteJournalEntriesByFlightId(@NotNull String flightId) {
    String sql = "DELETE FROM " + TABLE_NAME + " WHERE mutation->>'FLIGHT_ID' = :flightId";

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("flightId", flightId);

    int entriesRemoved = jdbcTemplate.update(sql, params);
    logger.warn("{} journal entries removed for flight id: {}", entriesRemoved, flightId);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<JournalEntryModel> retrieveEntriesByIdAndType(
      UUID resourceKey, @NotNull IamResourceType resourceType, long offset, int limit) {
    String sql =
        """
            SELECT %s FROM %s
            WHERE resource_key = :resource_key
            AND resource_type = :resource_type
            ORDER BY entry_timestamp DESC
            OFFSET :offset LIMIT :limit
            """
            .formatted(summaryQueryColumns, TABLE_NAME);

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("resource_key", resourceKey)
            .addValue("resource_type", resourceType.toString())
            .addValue("offset", offset)
            .addValue("limit", limit);
    return jdbcTemplate.query(sql, params, new JournalEntryMapper());
  }

  private class JournalEntryMapper implements RowMapper<JournalEntryModel> {
    @Override
    public JournalEntryModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      UUID journalId = rs.getObject("id", UUID.class);
      Map<?, ?> mutation = null;
      String rsMutation = rs.getString("mutation");
      if (rsMutation != null) {
        try {
          mutation = objectMapper.readValue(rsMutation, Map.class);
        } catch (JsonProcessingException e) {
          throw new CorruptMetadataException(
              String.format("Invalid mutation field for journal entry - id: %s", journalId), e);
        }
      }
      IamResourceType resourceType =
          IamResourceType.fromString(rs.getString("resource_type").toUpperCase());
      return new JournalEntry()
          .id(journalId)
          .user(rs.getString("user_email"))
          .resourceKey(rs.getObject("resource_key", UUID.class))
          .resourceType(resourceType)
          .className(rs.getString("class_name"))
          .methodName(rs.getString("method_name"))
          .entryType(JournalService.EntryType.valueOf(rs.getString("entry_type").toUpperCase()))
          .mutations(mutation)
          .note(rs.getString("note"))
          .when(rs.getTimestamp("entry_timestamp").toInstant())
          .toModel();
    }
  }
}
