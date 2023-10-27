package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import bio.terra.common.category.Unit;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class TableVariableTest {

  @Test
  void renderSQLForPrimary() {
    TableVariable tableVariable = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(tableVariable.renderSQL(SqlPlatform.BIGQUERY), equalToIgnoringCase("table as t"));
  }

  @Test
  void renderSQLForJoined() {
    TablePointer parentTable = TablePointer.fromTableName("parentTable");
    TableVariable parentTableVariable = TableVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer.Builder()
                .tablePointer(parentTable)
                .columnName("parentJoinField")
                .build(),
            parentTableVariable,
            null);
    TablePointer tablePointer = TablePointer.fromTableName("table");
    TableVariable tableVariable =
        TableVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    TableVariable.generateAliases(List.of(tableVariable, parentTableVariable));
    assertThat(
        tableVariable.renderSQL(SqlPlatform.BIGQUERY),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }
}
