package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.sql.Date;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class LiteralTest {

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderString(SqlRenderContext context) {
    var literal = new Literal("foo");
    assertThat(literal.renderSQL(context), is("'foo'"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderStringEscaped(SqlRenderContext context) {
    var literal = new Literal("foo's");
    assertThat(literal.renderSQL(context), is("'foo’s'"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderInt(SqlRenderContext context) {
    var literal = new Literal(42);
    assertThat(literal.renderSQL(context), is("42"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderBoolean(SqlRenderContext context) {
    var literal = new Literal(true);
    assertThat(literal.renderSQL(context), is(context.getPlatform().choose("true", "1")));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderDate(SqlRenderContext context) {
    var literal = new Literal(Date.valueOf("2021-01-01"));
    assertThat(literal.renderSQL(context), is("DATE('2021-01-01')"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderDouble(SqlRenderContext context) {
    var literal = new Literal(1.234);
    assertThat(literal.renderSQL(context), is("FLOAT('1.234')"));
  }

  @Test
  void renderNull() {
    assertThat(new Literal().renderSQL(null), is("NULL"));
  }
}
