package bio.terra.service.tags;

import bio.terra.common.DaoUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections4.ListUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

@Component
public class TagUtils {

  /**
   * @param requestedTags tags provided by a user at resource creation
   * @return distinct non-null tags from request (case-sensitive)
   */
  public static List<String> getDistinctTags(List<String> requestedTags) {
    return ListUtils.emptyIfNull(requestedTags).stream()
        .distinct()
        .filter(Objects::nonNull)
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
    clauses.add(String.format("%s.tags @> :tags", table));
  }
}
