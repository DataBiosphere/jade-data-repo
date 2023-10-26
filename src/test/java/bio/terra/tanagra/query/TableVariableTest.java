package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.query.datapointer.DataPointer;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class TableVariableTest {

  @Test
  void renderSQLForPrimary() {
    TableVariable tableVariable = TableVariable.forPrimary(createTable("table"));
    TableVariable.generateAliases(List.of(tableVariable));
    assertThat(tableVariable.renderSQL(SqlPlatform.BIGQUERY), equalToIgnoringCase("table as t"));
  }

  @Test
  void renderSQLForJoined() {
    TablePointer parentTable = createTable("parentTable");
    TableVariable parentTableVariable = TableVariable.forPrimary(parentTable);
    FieldVariable joinFieldOnParent =
        new FieldVariable(
            new FieldPointer.Builder()
                .tablePointer(parentTable)
                .columnName("parentJoinField")
                .build(),
            parentTableVariable,
            null);
    TablePointer tablePointer = createTable("table");
    TableVariable tableVariable =
        TableVariable.forJoined(tablePointer, "joinField", joinFieldOnParent);
    TableVariable.generateAliases(List.of(tableVariable, parentTableVariable));
    assertThat(
        tableVariable.renderSQL(SqlPlatform.BIGQUERY),
        equalToIgnoringCase("JOIN table AS t ON t.joinField = p.parentJoinField"));
  }

  @NotNull
  private static TablePointer createTable(String tableName) {
    return TablePointer.fromTableName(
        new DataPointer(DataPointer.Type.BQ_DATASET, null) {
          @Override
          public String getTableSQL(String tableName) {
            return tableName;
          }
        },
        tableName);
  }
}
