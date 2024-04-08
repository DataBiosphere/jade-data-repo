package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class BooleanAndOrFilterVariableTest {

  private final BooleanAndOrFilterVariable variable;

  BooleanAndOrFilterVariableTest() {
    TableVariable table1 = TableVariable.forPrimary(QueryTestUtils.fromTableName("table1"));
    TableVariable table2 = TableVariable.forPrimary(QueryTestUtils.fromTableName("table2"));
    variable =
        BooleanAndOrFilterVariable.and(
            new BinaryFilterVariable(
                table1.makeFieldVariable("field1"),
                BinaryFilterVariable.BinaryOperator.EQUALS,
                new Literal("value1")),
            new BinaryFilterVariable(
                table2.makeFieldVariable("field2"),
                BinaryFilterVariable.BinaryOperator.EQUALS,
                new Literal("value2")));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSQL(SqlRenderContext context) {
    assertThat(variable.renderSQL(context), is("(t.field1 = 'value1' AND t0.field2 = 'value2')"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSQLWorksWithNoSubQueries(SqlRenderContext renderContext) {
    assertThat(BooleanAndOrFilterVariable.and().renderSQL(renderContext), is("1=1"));
  }
}
