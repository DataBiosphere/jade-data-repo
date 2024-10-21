package bio.terra.service.tags;

import bio.terra.common.DaoUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

@Component
public class TagUtils {

  /**
   * @param requestedTags tags provided by a user at resource creation or modification
   * @return stripped nonempty distinct (case-sensitive) tags from request
   */
  public static List<String> sanitizeTags(List<String> requestedTags) {
    return ListUtils.emptyIfNull(requestedTags).stream()
        .map(StringUtils::strip)
        .filter(StringUtils::isNotEmpty)
        .distinct()
        .toList();
  }

  /** Adds a clause to restrict table rows to those containing all specified tags. */
  public static void addTagsClause(
      Connection connection,
      List<String> tags,
      MapSqlParameterSource params,
      List<String> clauses,
      String table)
      throws SQLException {
    params.addValue("tags", DaoUtils.createSqlStringArray(connection, tags));
    // Resources created before tags were introduced can have a null tags value:
    // We coalesce to an empty array for a clear true/false answer when assessing containment.
    clauses.add(String.format("COALESCE(%s.tags, ARRAY[]::TEXT[]) @> :tags", table));
  }
}
