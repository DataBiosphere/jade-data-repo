package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class OrderByVariableTest {

  @NotNull
  private static FieldVariable createVariable() {
    TablePointer table = TablePointer.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(table);
    return new FieldVariable(new FieldPointer(table, "column"), tableVariable);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLAsc(SqlRenderContext context) {
    var orderByVariable = new OrderByVariable(createVariable());
    assertThat(orderByVariable.renderSQL(false, context), is("t.column ASC"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLDesc(SqlRenderContext context) {
    var orderByVariable = new OrderByVariable(createVariable(), OrderByDirection.DESCENDING);
    assertThat(orderByVariable.renderSQL(false, context), is("t.column DESC"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLRandom(SqlRenderContext context) {
    var orderByVariable = OrderByVariable.random();
    assertThat(orderByVariable.renderSQL(true, context), is("RAND()"));
  }
}
