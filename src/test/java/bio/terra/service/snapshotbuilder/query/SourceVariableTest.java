package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class SourceVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLForPrimary(SqlRenderContext context) {
    SourceVariable sourceVariable = SourceVariable.forPrimary(TablePointer.fromTableName("table"));
    assertThat(sourceVariable.renderSQL(context), equalToIgnoringCase("table as t"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLForJoined(SqlRenderContext context) {
    TablePointer parentTable = TablePointer.fromTableName("parentTable");
    SourceVariable parentSourceVariable = SourceVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer(parentTable, "parentJoinField"), parentSourceVariable, null);
    TablePointer tablePointer = TablePointer.fromTableName("table");
    SourceVariable sourceVariable =
        SourceVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    assertThat(
        sourceVariable.renderSQL(context),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }
}
