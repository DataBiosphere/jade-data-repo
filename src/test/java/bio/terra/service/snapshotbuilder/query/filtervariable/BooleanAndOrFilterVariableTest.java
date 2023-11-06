package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BooleanAndOrFilterVariableTest {

  private final BooleanAndOrFilterVariable variable;

  BooleanAndOrFilterVariableTest() {
    TableVariable table1 = TableVariable.forPrimary(TablePointer.fromTableName("table1"));
    TableVariable table2 = TableVariable.forPrimary(TablePointer.fromTableName("table2"));
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

  @Test
  void renderSQL() {
    assertThat(variable.renderSQL(), is("(t.field1 = 'value1' AND t0.field2 = 'value2')"));
  }
}
