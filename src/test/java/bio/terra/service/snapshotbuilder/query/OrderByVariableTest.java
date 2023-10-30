package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class OrderByVariableTest {

  @NotNull
  private static FieldVariable createVariable() {
    TablePointer table = TablePointer.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(table);
    var fieldVariable = new FieldVariable(new FieldPointer(table, "column"), tableVariable);
    TableVariable.generateAliases(List.of(tableVariable));
    return fieldVariable;
  }

  @Test
  void renderSQLAsc() {
    var orderByVariable = new OrderByVariable(createVariable());
    assertThat(orderByVariable.renderSQL(null, false), is("t.column ASC"));
  }

  @Test
  void renderSQLDesc() {
    var orderByVariable = new OrderByVariable(createVariable(), OrderByDirection.DESCENDING);
    assertThat(orderByVariable.renderSQL(null, false), is("t.column DESC"));
  }

  @Test
  void renderSQLRandom() {
    var orderByVariable = OrderByVariable.random();
    assertThat(orderByVariable.renderSQL(null, true), is("RAND()"));
  }
}
