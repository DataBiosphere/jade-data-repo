package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterVariableTest {

  private static FieldVariable createField() {
    TableVariable tableVariable = TableVariable.forPrimary(QueryTestUtils.fromTableName("table"));
    TableVariable.generateAliases(List.of(tableVariable));
    return tableVariable.makeFieldVariable("column");
  }

  @Test
  void renderSQL() {
    var filter =
        new BinaryFilterVariable(
            createField(), BinaryFilterVariable.BinaryOperator.LESS_THAN, new Literal(1234));
    assertThat(filter.renderSQL(null), is("t.column < 1234"));
  }

  @Test
  void equals() {
    var filter = BinaryFilterVariable.equals(createField(), new Literal("foo"));
    assertThat(filter.renderSQL(null), is("t.column = 'foo'"));
  }

  @Test
  void notEquals() {
    var filter = BinaryFilterVariable.notEquals(createField(), new Literal("foo"));
    assertThat(filter.renderSQL(null), is("t.column != 'foo'"));
  }

  @Test
  void notNull() {
    var filter = BinaryFilterVariable.notNull(createField());
    assertThat(filter.renderSQL(null), is("t.column IS NOT NULL"));
  }
}
