package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.sql.Date;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class LiteralTest {
  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderString(CloudPlatform platform) {
    var literal = new Literal("foo");
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("'foo'"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderStringEscaped(CloudPlatform platform) {
    var literal = new Literal("foo's");
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("'foo’s'"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderInt(CloudPlatform platform) {
    var literal = new Literal(42);
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("42"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderBoolean(CloudPlatform platform) {
    var literal = new Literal(true);
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("true"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderDate(CloudPlatform platform) {
    var literal = new Literal(Date.valueOf("2021-01-01"));
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("DATE('2021-01-01')"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderDouble(CloudPlatform platform) {
    var literal = new Literal(1.234);
    assertThat(literal.renderSQL(CloudPlatformWrapper.of(platform)), is("FLOAT('1.234')"));
  }
}
