package bio.terra.service.common;

import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidFilterException;
import bio.terra.grammar.exception.InvalidQueryException;
import org.apache.commons.lang3.StringUtils;

public class QueryUtils {

  /**
   * Format and parse a user provided filter. This method forces the filter to start with "WHERE" -
   * it will add a WHERE clause if one is not provided.
   *
   * <p>We then validate the filter against the ANTLR SQL Grammar. By inserting the user provided
   * filter clause into a SQL SELECT statement skeleton ("select * from schema.table "), we're able
   * to give the ANTLR code a full SQL statement for it to parse and make sure it's a grammatically
   * correct filter clause.
   *
   * <p>NOTE on SQL Skeleton Parsing: ANTLR does facilitate parsing just the WHERE clause without
   * the SQL SELECT skeleton, but I found that it missed some errors because it fumbled at the end
   * of line statements. For example, it caught that "WHERE a = 1)" was invalid but not "WHERE (a =
   * 1". We would need to add to the grammar to handle parsing just the WHERE clause.
   *
   * @param filter User provided filter clause
   * @return Grammatically-correct filter clause with WHERE statement
   */
  public static String formatAndParseUserFilter(String filter) {
    if (StringUtils.isEmpty(filter)) return "";
    String filterWithWhere =
        (filter.matches("(?i)\\(?where\\s+.*\\)?")) ? filter : "WHERE (" + filter + ")";
    try {
      Query.parse("select * from schema.table " + filterWithWhere);
    } catch (InvalidQueryException ex) {
      throw new InvalidFilterException("Unable to parse user provided filter: " + filter);
    }
    return filterWithWhere;
  }
}
