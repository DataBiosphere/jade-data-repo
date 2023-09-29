package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.exception.SystemException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class FieldVariableTest {

  @Test
  void renderSQL() {
    var fieldPointer = new FieldPointer.Builder().columnName("field").build();
    var tableVariable =
        TableVariable.forPrimary(TablePointer.fromTableName(null, "table"));
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(new FieldVariable(fieldPointer, tableVariable).renderSQL(null), is("t.field"));

    assertThat(
        new FieldVariable(fieldPointer, tableVariable, "bar").renderSQL(null),
        is("t.field AS bar"));

    var fieldPointerForeignKey =
        new FieldPointer.Builder().foreignTablePointer(TablePointer.fromRawSql(null, null)).build();
    var fieldVariableForeignKey = new FieldVariable(fieldPointerForeignKey, tableVariable);
    assertThrows(SystemException.class, fieldVariableForeignKey::renderSQL);

    var fieldVariableFunctionWrapper =
        new FieldVariable(
            new FieldPointer.Builder().sqlFunctionWrapper("foo").columnName("field").build(),
            tableVariable,
            "alias");
    assertThat(fieldVariableFunctionWrapper.renderSQL(null), is("foo(t.field)"));

    var fieldVariableSqlFunctionWrapper =
        new FieldVariable(
            new FieldPointer.Builder()
                .sqlFunctionWrapper("custom(<fieldSql>)")
                .columnName("field")
                .build(),
            tableVariable,
            "alias");
    assertThat(fieldVariableSqlFunctionWrapper.renderSQL(null), is("custom(t.field)"));
  }

  @Test
  void renderSqlForOrderBy() {
    var tableVariable =
        TableVariable.forPrimary(TablePointer.fromTableName(null, "table"));
    TableVariable.generateAliases(List.of(tableVariable));
    var fieldVariableFunctionWrapper =
        new FieldVariable(
            new FieldPointer.Builder().sqlFunctionWrapper("foo").columnName("field").build(),
            tableVariable,
            "alias");
    assertThat(fieldVariableFunctionWrapper.renderSqlForOrderBy(), is("t.field"));
  }

  @Test
  void renderSqlForWhere() {
    var fieldPointer = new FieldPointer.Builder().columnName("field").build();
    var tableVariable =
        TableVariable.forPrimary(TablePointer.fromTableName(null, "table"));
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
    var fieldPointer = new FieldPointer.Builder().columnName("foo").build();
    assertThat(new FieldVariable(fieldPointer, null).getAliasOrColumnName(), is("foo"));
    assertThat(new FieldVariable(fieldPointer, null, "bar").getAliasOrColumnName(), is("bar"));
    var fieldPointerForeignKey =
        new FieldPointer.Builder()
            .foreignTablePointer(TablePointer.fromTableName(null, null))
            .foreignColumnName("baz")
            .build();
    assertThat(new FieldVariable(fieldPointerForeignKey, null).getAliasOrColumnName(), is("baz"));
  }

  @Test
  void getFieldPointer() {
    var fieldPointer = new FieldPointer.Builder().build();
    assertThat(new FieldVariable(fieldPointer, null).getFieldPointer(), is(fieldPointer));
  }
}
