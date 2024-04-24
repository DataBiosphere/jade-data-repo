package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class TableVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLForPrimary(SqlRenderContext context) {
    TableVariable tableVariable = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    assertThat(tableVariable.renderSQL(context), equalToIgnoringCase("table as t"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLForJoined(SqlRenderContext context) {
    TablePointer parentTable = TablePointer.fromTableName("parentTable");
    TableVariable parentTableVariable = TableVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer(parentTable, "parentJoinField"), parentTableVariable, null);
    TablePointer tablePointer = TablePointer.fromTableName("table");
    TableVariable tableVariable =
        TableVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    assertThat(
        tableVariable.renderSQL(context),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }
}
