package bio.terra.common;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public final class DaoUtils {

  private DaoUtils() {}

  public static String orderByClause(
      EnumerateSortByParam sort, SqlSortDirection direction, String table) {
    if (sort == null || direction == null) {
      return "";
    }
    return String.format(" ORDER BY %s.%s %s ", table, sort, direction);
  }

  public static void addFilterClause(
      String filter, MapSqlParameterSource params, List<String> clauses, String table) {
    if (!StringUtils.isEmpty(filter)) {
      params.addValue("filter", DaoUtils.escapeFilter(filter));
      clauses.add(
          String.format(" (%s.name ILIKE :filter OR %s.description ILIKE :filter) ", table, table));
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
    return connection.createArrayOf("text", list.toArray());
  }

  public static List<String> getStringList(ResultSet rs, String column) throws SQLException {
    Array sqlArray = rs.getArray(column);
    if (sqlArray == null) {
      return List.of();
    }
    return List.of((String[]) sqlArray.getArray());
  }

  public static List<String> getJsonStringArray(
      ResultSet rs, String column, ObjectMapper objectMapper)
      throws SQLException, JsonProcessingException {
    String jsonArrayRaw = rs.getString(column);
    if (jsonArrayRaw != null) {
      return objectMapper.readValue(jsonArrayRaw, new TypeReference<>() {});
    } else {
      return List.of();
    }
  }

  // Based on Exception returned, determine if we should attempt to retry an operation/stairway step
  public static boolean retryQuery(DataAccessException dataAccessException) {
    return ExceptionUtils.hasCause(dataAccessException, RecoverableDataAccessException.class)
        || ExceptionUtils.hasCause(dataAccessException, TransientDataAccessException.class);
  }
}
