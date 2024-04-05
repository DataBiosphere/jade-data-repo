package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class FieldVariableTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    var table = QueryTestUtils.fromTableName("table");

    var fieldPointer = new FieldPointer(table, "field");
    var tableVariable = TableVariable.forPrimary(table);
    var cloudPlatformWrapper = CloudPlatformWrapper.of(platform);
    SqlRenderContext renderContext = QueryTestUtils.createContext(platform);
    assertThat(
        new FieldVariable(fieldPointer, tableVariable).renderSQL(renderContext), is("t.field"));

    assertThat(
        new FieldVariable(fieldPointer, tableVariable, "bar").renderSQL(renderContext),
        is("t.field AS bar"));

    var fieldPointerForeignKey = FieldPointer.foreignColumn(TablePointer.fromRawSql(null), null);
    var fieldVariableForeignKey = new FieldVariable(fieldPointerForeignKey, tableVariable);
    assertThrows(
        UnsupportedOperationException.class,
        () -> fieldVariableForeignKey.renderSQL(renderContext));

    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(fieldVariableFunctionWrapper.renderSQL(renderContext), is("foo(t.field) AS alias"));

    var fieldVariableSqlFunctionWrapper =
        new FieldVariable(
            new FieldPointer(table, "field", "custom(<fieldSql>)"), tableVariable, "alias");
    assertThat(
        fieldVariableSqlFunctionWrapper.renderSQL(renderContext), is("custom(t.field) AS alias"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLForAliasAndDistinct(CloudPlatform platform) {
    var table = QueryTestUtils.fromTableName("table");
    var tableVariable = TableVariable.forPrimary(table);

    var fieldVariable =
        new FieldVariable(new FieldPointer(table, "field", "COUNT"), tableVariable, "count", true);

    assertThat(
        fieldVariable.renderSQL(QueryTestUtils.createContext(platform)),
        is("COUNT(DISTINCT t.field) AS count"));
  }

  @Test
  void renderSqlForOrderBy() {
    var table = QueryTestUtils.fromTableName("table");
    var tableVariable = TableVariable.forPrimary(table);
    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(
        fieldVariableFunctionWrapper.renderSqlForOrderOrGroupBy(
            false, QueryTestUtils.createContext(CloudPlatform.GCP)),
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
        FieldPointer.foreignColumn(QueryTestUtils.fromTableName(null), "baz");
    assertThat(new FieldVariable(fieldPointerForeignKey, null).getAliasOrColumnName(), is("baz"));
  }

  @Test
  void getFieldPointer() {
    var fieldPointer = new FieldPointer(null, null);
    assertThat(new FieldVariable(fieldPointer, null).getFieldPointer(), is(fieldPointer));
  }
}
