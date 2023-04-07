package bio.terra.common;

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

  /**
   * @param add tags to add to a resource
   * @param remove tags to remove from a resource
   * @param params SQL parameters to supplement as needed to populate the returned SQL expression
   * @return a SQL expression to add and/or remove tags to a resource's existing tag set, with
   *     duplicates and nulls removed
   */
  public static String updateTagsExpression(
      Connection connection, List<String> add, List<String> remove, MapSqlParameterSource params)
      throws SQLException {
    // Note: set operations (UNION, EXCEPT) return distinct rows.
    String expression =
        """
            (SELECT ARRAY_AGG(tag) FROM
              (SELECT UNNEST(tags) tag                  -- Start with existing tags
                UNION SELECT UNNEST(:tagsToAdd) tag     -- Include tags to add
                EXCEPT SELECT UNNEST(:tagsToRemove) tag -- Exclude tags to remove
                ORDER BY tag ASC) updated               -- Set operations may not preserve order
            WHERE tag IS NOT NULL)
            """;

    params
        .addValue("tagsToAdd", DaoUtils.createSqlStringArray(connection, add))
        .addValue("tagsToRemove", DaoUtils.createSqlStringArray(connection, remove));
    return expression;
  }
}
