package bio.terra.service.common;

import bio.terra.grammar.Query;
import org.apache.commons.lang3.StringUtils;

public class QueryUtils {
  public static String formatAndParseUserFilter(String filter) {
    if (StringUtils.isEmpty(filter)) return "";
    String filterWithWhere =
        (filter.matches("(?i)\\(?where\\s+.*\\)?")) ? filter : "WHERE (" + filter + ")";
    // Parse a Sql skeleton with the filter
    // Note: We could specifically parse the where statement, but it doesn't catch as many errors
    Query.parse("select * from schema.table " + filterWithWhere);
    return filterWithWhere;
  }
}
