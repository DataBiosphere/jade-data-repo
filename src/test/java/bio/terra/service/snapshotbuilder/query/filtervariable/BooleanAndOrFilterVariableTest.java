package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class BooleanAndOrFilterVariableTest {

  private final BooleanAndOrFilterVariable variable;

  BooleanAndOrFilterVariableTest() {
    TableVariable table1 = TableVariable.forPrimary(QueryTestUtils.fromTableName("table1"));
    TableVariable table2 = TableVariable.forPrimary(QueryTestUtils.fromTableName("table2"));
    TableVariable.generateAliases(List.of(table1, table2));
    variable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND,
            List.of(
                new BinaryFilterVariable(
                    new FieldVariable(new FieldPointer(null, "field1"), table1),
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("value1")),
                new BinaryFilterVariable(
                    new FieldVariable(new FieldPointer(null, "field2"), table2),
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("value2"))));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    assertThat(
        variable.renderSQL(CloudPlatformWrapper.of(platform)),
        is("(t.field1 = 'value1' AND t0.field2 = 'value2')"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLWorksWithNoSubqueries(CloudPlatform platform) {
    assertThat(
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, List.of())
            .renderSQL(CloudPlatformWrapper.of(platform)),
        is("1=1"));
  }
}
