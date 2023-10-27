package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterVariableTest {

  @Test
  void renderSQL() {
    var binaryOperator = BinaryFilterVariable.BinaryOperator.EQUALS;
    var literal = new Literal("foo");
    TableVariable tableVariable = TableVariable.forPrimary(new TablePointer("table", null, null));
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
