package bio.terra.service.snapshotbuilder.query;

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
    TableVariable tableVariable = TableVariable.forPrimary(QueryTestUtils.fromTableName("table"));
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(tableVariable.renderSQL(), equalToIgnoringCase("table as t"));
  }

  @Test
  void renderSQLForJoined() {
    TablePointer parentTable = QueryTestUtils.fromTableName("parentTable");
    TableVariable parentTableVariable = TableVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer(parentTable, "parentJoinField"), parentTableVariable, null);
    TablePointer tablePointer = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable =
        TableVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    TableVariable.generateAliases(List.of(tableVariable, parentTableVariable));
    assertThat(
        tableVariable.renderSQL(),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }
}
