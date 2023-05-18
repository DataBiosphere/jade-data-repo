package bio.terra.service.common;

import bio.terra.grammar.FilterQuery;
import org.apache.commons.lang3.StringUtils;

public class QueryUtils {
  public static String whereClause(String filter) {
    if (StringUtils.isEmpty(filter)) return "";
    String filterWithWhere =
        (filter.matches("(?i)\\(?where\\s+.*\\)?")) ? filter : "WHERE (" + filter + ")";
    FilterQuery.parseWhereClause(filterWithWhere);
    return filterWithWhere;
  }
}
