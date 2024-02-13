package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import jakarta.validation.constraints.NotNull;
import bio.terra.model.CloudPlatform;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class OrderByVariableTest {

  @NotNull
  private static FieldVariable createVariable() {
    TablePointer table = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(table);
    var fieldVariable = new FieldVariable(new FieldPointer(table, "column"), tableVariable);
    TableVariable.generateAliases(List.of(tableVariable));
    return fieldVariable;
  }

  @Test
  void renderSQLAsc() {
    var orderByVariable = new OrderByVariable(createVariable());
    assertThat(orderByVariable.renderSQL(false), is("t.column ASC"));
  }

  @Test
  void renderSQLDesc() {
    var orderByVariable = new OrderByVariable(createVariable(), OrderByDirection.DESCENDING);
    assertThat(orderByVariable.renderSQL(false), is("t.column DESC"));
  }

  @Test
  void renderSQLRandom() {
    var orderByVariable = OrderByVariable.random();
    assertThat(orderByVariable.renderSQL(true), is("RAND()"));
  }
}
