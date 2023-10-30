package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.sql.Date;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class LiteralTest {
  @Test
  void renderString() {
    var literal = new Literal("foo");
    assertThat(literal.renderSQL(null), is("'foo'"));
  }

  @Test
  void renderStringEscaped() {
    var literal = new Literal("foo's");
    assertThat(literal.renderSQL(null), is("'foo’s'"));
  }

  @Test
  void renderInt() {
    var literal = new Literal(42);
    assertThat(literal.renderSQL(null), is("42"));
  }

  @Test
  void renderBoolean() {
    var literal = new Literal(true);
    assertThat(literal.renderSQL(null), is("true"));
  }

  @Test
  void renderDate() {
    var literal = new Literal(Date.valueOf("2021-01-01"));
    assertThat(literal.renderSQL(null), is("DATE('2021-01-01')"));
  }

  @Test
  void renderDouble() {
    var literal = new Literal(3.14);
    assertThat(literal.renderSQL(null), is("FLOAT('3.14')"));
  }
}
