package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.sql.Date;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(Unit.TAG)
class LiteralTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderString(SqlRenderContext context) {
    var literal = new Literal("foo");
    assertThat(literal.renderSQL(context), is("'foo'"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderStringEscaped(SqlRenderContext context) {
    var literal = new Literal("foo's");
    assertThat(literal.renderSQL(context), is("'foo’s'"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderInt(SqlRenderContext context) {
    var literal = new Literal(42);
    assertThat(literal.renderSQL(context), is("42"));
  }

  public static Stream<Arguments> renderBoolean() {
    return Stream.of(
        Arguments.of(SqlRenderContextProvider.of(CloudPlatform.GCP), true, "true"),
        Arguments.of(SqlRenderContextProvider.of(CloudPlatform.GCP), false, "false"),
        Arguments.of(SqlRenderContextProvider.of(CloudPlatform.AZURE), true, "1"),
        Arguments.of(SqlRenderContextProvider.of(CloudPlatform.AZURE), false, "0"));
  }

  @ParameterizedTest
  @MethodSource
  void renderBoolean(SqlRenderContext context, boolean value, String expected) {
    assertThat(new Literal(value).renderSQL(context), is(expected));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderDate(SqlRenderContext context) {
    var literal = new Literal(Date.valueOf("2021-01-01"));
    assertThat(literal.renderSQL(context), is("DATE('2021-01-01')"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderDouble(SqlRenderContext context) {
    var literal = new Literal(1.234);
    assertThat(literal.renderSQL(context), is("FLOAT('1.234')"));
  }

  @Test
  void renderNull() {
    assertThat(new Literal().renderSQL(null), is("NULL"));
  }
}
