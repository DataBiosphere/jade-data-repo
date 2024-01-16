package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterVariableTest {

  @Test
  void renderSQL() {
    var binaryOperator = BinaryFilterVariable.BinaryOperator.EQUALS;
    var literal = new Literal("foo");
    TableVariable tableVariable = TableVariable.forPrimary(QueryTestUtils.fromTableName("table"));
    TableVariable.generateAliases(List.of(tableVariable));
    var filterVariable =
        new BinaryFilterVariable(
            new FieldVariable(new FieldPointer(null, "column"), tableVariable),
            binaryOperator,
            literal);
    assertThat(filterVariable.renderSQL(), is("t.column = 'foo'"));
  }
}
