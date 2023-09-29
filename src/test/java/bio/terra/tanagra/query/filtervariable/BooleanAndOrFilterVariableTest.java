package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BooleanAndOrFilterVariableTest {

  private final FieldVariable fieldVariable1;
  private final FieldVariable fieldVariable2;
  private final BooleanAndOrFilterVariable variable;

  BooleanAndOrFilterVariableTest() {
    TableVariable table1 =
        TableVariable.forPrimary(TablePointer.fromTableName(null, "table1"));
    fieldVariable1 =
        new FieldVariable(new FieldPointer.Builder().columnName("field1").build(), table1);
    TableVariable table2 =
        TableVariable.forPrimary(TablePointer.fromTableName(null, "table2"));
    fieldVariable2 =
        new FieldVariable(new FieldPointer.Builder().columnName("field2").build(), table2);
    TableVariable.generateAliases(List.of(table1, table2));
    variable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND,
            List.of(
                new BinaryFilterVariable(
                    fieldVariable1,
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("value1")),
                new BinaryFilterVariable(
                    fieldVariable2,
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("value2"))));
  }

  @Test
  void getSubstitutionTemplate() {
    assertThrows(UnsupportedOperationException.class, () -> variable.getSubstitutionTemplate(null));
  }

  @Test
  void getFieldVariables() {
    assertThat(variable.getFieldVariables(), containsInAnyOrder(fieldVariable1, fieldVariable2));
  }

  @Test
  void renderSQL() {
    assertThat(variable.renderSQL(null), is("(t.field1 = 'value1' AND t0.field2 = 'value2')"));
  }
}
