package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class TableVariableTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLForPrimary(CloudPlatform platform) {
    TableVariable tableVariable = TableVariable.forPrimary(QueryTestUtils.fromTableName("table"));
    assertThat(
        tableVariable.renderSQL(QueryTestUtils.createContext(platform)),
        equalToIgnoringCase("table as t"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLForJoined(CloudPlatform platform) {
    TablePointer parentTable = QueryTestUtils.fromTableName("parentTable");
    TableVariable parentTableVariable = TableVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer(parentTable, "parentJoinField"), parentTableVariable, null);
    TablePointer tablePointer = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable =
        TableVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    assertThat(
        tableVariable.renderSQL(QueryTestUtils.createContext(platform)),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }
}
