package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterVariableTest {

  private static FieldVariable createField() {
    TableVariable tableVariable = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    return tableVariable.makeFieldVariable("column");
  }

  @Test
  void renderSQL() {
    var filter =
        new BinaryFilterVariable(
            createField(), BinaryFilterVariable.BinaryOperator.LESS_THAN, new Literal(1234));
    assertThat(
        filter.renderSQL(SqlRenderContextProvider.of(CloudPlatform.AZURE)), is("t.column < 1234"));
  }

  @Test
  void equals() {
    var filter = BinaryFilterVariable.equals(createField(), new Literal("foo"));
    assertThat(
        filter.renderSQL(SqlRenderContextProvider.of(CloudPlatform.AZURE)), is("t.column = 'foo'"));
  }

  @Test
  void notEquals() {
    var filter = BinaryFilterVariable.notEquals(createField(), new Literal("foo"));
    assertThat(
        filter.renderSQL(SqlRenderContextProvider.of(CloudPlatform.AZURE)),
        is("t.column != 'foo'"));
  }

  @Test
  void notNull() {
    var filter = BinaryFilterVariable.notNull(createField());
    assertThat(
        filter.renderSQL(SqlRenderContextProvider.of(CloudPlatform.AZURE)),
        is("t.column IS NOT NULL"));
  }
}
