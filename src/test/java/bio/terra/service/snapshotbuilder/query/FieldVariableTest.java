package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class FieldVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    var table = TablePointer.fromTableName("table");

    var fieldPointer = new FieldPointer(table, "field");
    var tableVariable = TableVariable.forPrimary(table);
    assertThat(new FieldVariable(fieldPointer, tableVariable).renderSQL(context), is("t.field"));

    assertThat(
        new FieldVariable(fieldPointer, tableVariable, "bar").renderSQL(context),
        is("t.field AS bar"));

    var fieldPointerForeignKey =
        FieldPointer.foreignColumn(TablePointer.fromTableName("table"), "column");
    var fieldVariableForeignKey = new FieldVariable(fieldPointerForeignKey, tableVariable);
    assertThrows(
        UnsupportedOperationException.class, () -> fieldVariableForeignKey.renderSQL(context));

    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(fieldVariableFunctionWrapper.renderSQL(context), is("foo(t.field) AS alias"));

    var fieldVariableSqlFunctionWrapper =
        new FieldVariable(
            new FieldPointer(table, "field", "custom(<fieldSql>)"), tableVariable, "alias");
    assertThat(fieldVariableSqlFunctionWrapper.renderSQL(context), is("custom(t.field) AS alias"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLForAliasAndDistinct(SqlRenderContext context) {
    var table = TablePointer.fromTableName("table");
    var tableVariable = TableVariable.forPrimary(table);

    var fieldVariable =
        new FieldVariable(
            new FieldPointer(table, "field", "COUNT"),
            tableVariable,
            QueryBuilderFactory.COUNT,
            true);

    assertThat(fieldVariable.renderSQL(context), is("COUNT(DISTINCT t.field) AS count"));
  }

  @Test
  void renderSqlForOrderBy() {
    var table = TablePointer.fromTableName("table");
    var tableVariable = TableVariable.forPrimary(table);
    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(
        fieldVariableFunctionWrapper.renderSqlForOrderOrGroupBy(
            false, SqlRenderContextProvider.of(CloudPlatform.GCP)),
        is("foo(t.field) AS alias"));
  }

  @Test
  void getAlias() {
    assertThat(new FieldVariable(null, null).getAlias(), is(""));
    assertThat(new FieldVariable(null, null, "bar").getAlias(), is("bar"));
  }

  @Test
  void getAliasOrColumnName() {
    var fieldPointer = new FieldPointer(null, "foo");
    assertThat(new FieldVariable(fieldPointer, null).getAliasOrColumnName(), is("foo"));
    assertThat(new FieldVariable(fieldPointer, null, "bar").getAliasOrColumnName(), is("bar"));
    var fieldPointerForeignKey =
        FieldPointer.foreignColumn(TablePointer.fromTableName(null), "baz");
    assertThat(new FieldVariable(fieldPointerForeignKey, null).getAliasOrColumnName(), is("baz"));
  }

  @Test
  void getFieldPointer() {
    var fieldPointer = new FieldPointer(null, null);
    assertThat(new FieldVariable(fieldPointer, null).getFieldPointer(), is(fieldPointer));
  }
}
