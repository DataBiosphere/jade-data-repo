package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import bio.terra.grammar.exception.InvalidFilterException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("bio.terra.common.category.Unit")
class QueryUtilsUnitTest {

  @Test
  void testWhereClause() {
    String filterWithWhere = "WHERE ( a = 1 )";
    assertThat(
        "Where clause should not add another 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  void testWhereClauseLowercase() {
    String filterWithWhere = "where ( a = 1 )";
    assertThat(
        "Where clause should not add another 'where' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  void testWhereClauseNoParen() {
    String filterWithWhere = "WHERE a = 1 ";
    assertThat(
        "Where clause should not add another 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  void testWhereClauseNoWHERE() {
    String filterNoWhere = "a = 1";
    String filterWithWhere = "WHERE (a = 1)";
    assertThat(
        "Where clause should add the 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterNoWhere),
        equalTo(filterWithWhere));
  }

  @Test
  void testWhereClauseNullFilter() {
    assertThat("return empty filter", QueryUtils.formatAndParseUserFilter(null), equalTo(""));
  }

  @Test
  void testWhereClauseEmptyFilter() {
    assertThat("return empty filter", QueryUtils.formatAndParseUserFilter(""), equalTo(""));
  }

  @Test
  void testInvalidWhereClause_MisspelledWhere() {
    String misspelledWhere = "WERE a = 1";
    assertThrows(
        InvalidFilterException.class, () -> QueryUtils.formatAndParseUserFilter(misspelledWhere));
  }

  @Test
  void testInvalidWhereClause_MissingParenAtEnd() {
    String missingParen = "WHERE (a = 1";
    assertThrows(
        InvalidFilterException.class, () -> QueryUtils.formatAndParseUserFilter(missingParen));
  }

  @Test
  void testInvalidWhereClause_MissingParen() {
    String missingParen = "WHERE a = 1)";
    assertThrows(
        InvalidFilterException.class, () -> QueryUtils.formatAndParseUserFilter(missingParen));
  }
}
