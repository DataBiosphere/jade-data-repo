package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class FieldVariableTest {

  @Test
  void renderSQL() {
    var table = TablePointer.fromTableName("table");

    var fieldPointer = new FieldPointer(table, "field");
    var tableVariable = TableVariable.forPrimary(table);
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(new FieldVariable(fieldPointer, tableVariable).renderSQL(), is("t.field"));

    assertThat(
        new FieldVariable(fieldPointer, tableVariable, "bar").renderSQL(), is("t.field AS bar"));

    var fieldPointerForeignKey = FieldPointer.foreignColumn(TablePointer.fromRawSql(null), null);
    var fieldVariableForeignKey = new FieldVariable(fieldPointerForeignKey, tableVariable);
    assertThrows(UnsupportedOperationException.class, () -> fieldVariableForeignKey.renderSQL());

    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(fieldVariableFunctionWrapper.renderSQL(), is("foo(t.field)"));

    var fieldVariableSqlFunctionWrapper =
        new FieldVariable(
            new FieldPointer(table, "field", "custom(<fieldSql>)"), tableVariable, "alias");
    assertThat(fieldVariableSqlFunctionWrapper.renderSQL(), is("custom(t.field)"));
  }

  @Test
  void renderSqlForOrderBy() {
    var table = TablePointer.fromTableName("table");
    var tableVariable = TableVariable.forPrimary(table);
    TableVariable.generateAliases(List.of(tableVariable));
    var fieldVariableFunctionWrapper =
        new FieldVariable(new FieldPointer(table, "field", "foo"), tableVariable, "alias");
    assertThat(fieldVariableFunctionWrapper.renderSqlForOrderOrGroupBy(false), is("foo(t.field)"));
  }

  @Test
  void renderSqlForWhere() {
    TablePointer table = TablePointer.fromTableName("table");
    var fieldPointer = new FieldPointer(table, "field");
    var tableVariable = TableVariable.forPrimary(table);
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(
        new FieldVariable(fieldPointer, tableVariable, "bar").renderSqlForWhere(), is("t.field"));
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
