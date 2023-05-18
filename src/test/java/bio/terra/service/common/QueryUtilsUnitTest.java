package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.grammar.exception.InvalidQueryException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class QueryUtilsUnitTest {

  @Test
  public void testWhereClause() {
    String filterWithWhere = "WHERE ( a = 1 )";
    assertThat(
        "Where clause should not add another 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  public void testWhereClauseLowercase() {
    String filterWithWhere = "where ( a = 1 )";
    assertThat(
        "Where clause should not add another 'where' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  public void testWhereClauseNoParen() {
    String filterWithWhere = "WHERE a = 1 ";
    assertThat(
        "Where clause should not add another 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterWithWhere),
        equalTo(filterWithWhere));
  }

  @Test
  public void testWhereClauseNoWHERE() {
    String filterNoWhere = "a = 1";
    String filterWithWhere = "WHERE (a = 1)";
    assertThat(
        "Where clause should add the 'WHERE' statement",
        QueryUtils.formatAndParseUserFilter(filterNoWhere),
        equalTo(filterWithWhere));
  }

  @Test
  public void testWhereClauseNullFilter() {
    assertThat("return empty filter", QueryUtils.formatAndParseUserFilter(null), equalTo(""));
  }

  @Test
  public void testWhereClauseEmptyFilter() {
    assertThat("return empty filter", QueryUtils.formatAndParseUserFilter(""), equalTo(""));
  }

  @Test
  public void testInvalidWhereClause_MisspelledWhere() {
    String misspelledWhere = "WERE a = 1";
    assertThrows(InvalidQueryException.class, () -> QueryUtils.formatAndParseUserFilter(misspelledWhere));
  }

  @Test
  public void testInvalidWhereClause_MissingParenAtEnd() {
    String missingParen = "WHERE (a = 1";
    assertThrows(InvalidQueryException.class, () -> QueryUtils.formatAndParseUserFilter(missingParen));
  }

  @Test
  public void testInvalidWhereClause_MissingParen() {
    String missingParen = "WHERE a = 1)";
    assertThrows(InvalidQueryException.class, () -> QueryUtils.formatAndParseUserFilter(missingParen));
  }
}
