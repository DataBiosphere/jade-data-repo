package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
class BinaryFilterVariableTest {

  @Test
  void renderSQL() {
    var binaryOperator = BinaryFilterVariable.BinaryOperator.EQUALS;
    var literal = new Literal("foo");
    TableVariable tableVariable =
        TableVariable.forPrimary(new TablePointer(null, "table", null, null));
    TableVariable.generateAliases(List.of(tableVariable));
    var filterVariable =
        new BinaryFilterVariable(
            new FieldVariable(
                new FieldPointer.Builder().columnName("column").build(), tableVariable),
            binaryOperator,
            literal);
    assertThat(filterVariable.renderSQL(null), is("t.column = 'foo'"));
  }
}
