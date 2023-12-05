package bio.terra.common;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public final class DaoUtils {

  private DaoUtils() {}

  public static String orderByClause(
      EnumerateSortByParam sort, SqlSortDirection direction, String table) {
    EnumerateSortByParam sortToUse;
    SqlSortDirection directionToUse;

    if (sort == null || direction == null) {
      sortToUse = EnumerateSortByParam.CREATED_DATE;
      directionToUse = SqlSortDirection.DESC;
    } else {
      sortToUse = sort;
      directionToUse = direction;
    }

    return String.format(" ORDER BY %s.%s %s ", table, sortToUse, directionToUse);
  }

  public static void addFilterClause(
      String filter, MapSqlParameterSource params, List<String> clauses, String table) {
    if (!StringUtils.isEmpty(filter)) {
      params.addValue("filter", DaoUtils.escapeFilter(filter));
      clauses.add(
          String.format(
              " (%s.id::text ILIKE :filter OR %s.name ILIKE :filter OR %s.description ILIKE :filter) ",
              table, table, table));
    }
  }

  public static void addRegionFilterClause(
      String region, MapSqlParameterSource params, List<String> clauses, String datasetIdField) {
    if (!StringUtils.isEmpty(region)) {
      GoogleRegion regionFilter = GoogleRegion.fromValue(region);
      if (regionFilter != null) {
        params.addValue("region", escapeFilter(regionFilter.name()));
        clauses.add(
            String.format(
                " exists (SELECT 1 FROM storage_resource"
                    + " WHERE storage_resource.dataset_id = %s AND storage_resource.region ILIKE :region) ",
                datasetIdField));
      }
    }
  }

  public static void addAuthzIdsClause(
      Collection<UUID> authzIds, MapSqlParameterSource params, List<String> clauses, String table) {
    params.addValue("idlist", authzIds);
    clauses.add(String.format(" %s.id in (:idlist) ", table));
  }

  public static String escapeFilter(String filter) {
    StringBuilder builder = new StringBuilder("%");
    for (char c : filter.toCharArray()) {
      if (c == '_' || c == '%') {
        builder.append('\\');
      }
      builder.append(c);
    }
    return builder.append('%').toString();
  }

  public static Array createSqlStringArray(Connection connection, List<String> list)
      throws SQLException {
    Object[] array = ListUtils.emptyIfNull(list).toArray();
    return connection.createArrayOf("text", array);
  }

  public static List<String> getStringList(ResultSet rs, String column) throws SQLException {
    Array sqlArray = rs.getArray(column);
    if (sqlArray == null) {
      return List.of();
    }
    return List.of((String[]) sqlArray.getArray());
  }

  // Based on Exception returned, determine if we should attempt to retry an operation/stairway step
  public static boolean retryQuery(DataAccessException dataAccessException) {
    return ExceptionUtils.hasCause(dataAccessException, RecoverableDataAccessException.class)
        || ExceptionUtils.hasCause(dataAccessException, TransientDataAccessException.class);
  }

  public static String propertiesToString(ObjectMapper objectMapper, Object properties) {
    if (properties != null) {
      try {
        return objectMapper.writeValueAsString(properties);
      } catch (JsonProcessingException ex) {
        throw new InvalidDatasetException("Invalid dataset properties: " + properties, ex);
      }
    } else {
      return null;
    }
  }

  public static Object stringToProperties(ObjectMapper objectMapper, String properties) {
    if (properties != null) {
      try {
        return objectMapper.readValue(properties, new TypeReference<>() {});
      } catch (JsonProcessingException e) {
        throw new CorruptMetadataException("Invalid properties field");
      }
    } else {
      return null;
    }
  }

  public static String getInstantString(ResultSet rs, String columnLabel) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(columnLabel);
    if (timestamp != null) {
      return timestamp.toInstant().toString();
    }
    return null;
  }

  public static class UuidMapper implements RowMapper<UUID> {
    private final String columnLabel;

    public UuidMapper(String columnLabel) {
      this.columnLabel = columnLabel;
    }

    @Override
    public UUID mapRow(ResultSet rs, int rowNum) throws SQLException {
      return rs.getObject(this.columnLabel, UUID.class);
    }
  }
}
