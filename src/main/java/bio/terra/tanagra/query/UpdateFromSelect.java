package bio.terra.tanagra.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class UpdateFromSelect implements SQLExpression {
  private final TableVariable updateTable;
  private final Map<FieldVariable, FieldVariable> setFields;
  private final Query selectQuery;
  private final FieldVariable updateJoinField;
  private final FieldVariable selectJoinField;

  public UpdateFromSelect(
      TableVariable updateTable,
      Map<FieldVariable, FieldVariable> setFields,
      Query selectQuery,
      FieldVariable updateJoinField,
      FieldVariable selectJoinField) {
    this.updateTable = updateTable;
    this.setFields = setFields;
    this.selectQuery = selectQuery;
    this.updateJoinField = updateJoinField;
    this.selectJoinField = selectJoinField;

    // Check that the select join field is part of the query.
    if (!selectQuery.getSelect().contains(selectJoinField)) {
      throw new IllegalArgumentException(
          "Select join field is not part of the query selected fields");
    }
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    // Build a table variable for a nested query.
    List<TableVariable> tableVars = new ArrayList<>();
    TablePointer nestedTable =
        TablePointer.fromRawSql(
            selectQuery.getPrimaryTable().getTablePointer().dataPointer(),
            selectQuery.renderSQL(platform));
    TableVariable nestedTableVar = TableVariable.forPrimary(nestedTable);
    tableVars.add(nestedTableVar);

    // Update the select set fields to point to the nested query table.
    Map<FieldVariable, FieldVariable> setFieldsForNestedVars = new HashMap<>();
    setFields.forEach(
        (updateSetField, selectSetField) -> {
          FieldPointer selectSetFieldForNested =
              new FieldPointer.Builder()
                  .tablePointer(nestedTable)
                  .columnName(selectSetField.getAliasOrColumnName())
                  .build();
          FieldVariable selectSetFieldForNestedVar =
              selectSetFieldForNested.buildVariable(nestedTableVar, tableVars);
          setFieldsForNestedVars.put(updateSetField, selectSetFieldForNestedVar);
        });

    // Update the select join field to point to the nested query table.
    FieldPointer selectJoinFieldForNested =
        new FieldPointer.Builder()
            .tablePointer(nestedTable)
            .columnName(selectJoinField.getAliasOrColumnName())
            .build();
    FieldVariable selectJoinFieldForNestedVar =
        selectJoinFieldForNested.buildVariable(nestedTableVar, tableVars);

    // generate a unique alias for both the select and update TableVariables
    TableVariable.generateAliases(List.of(updateTable, nestedTableVar));

    // Render each set field variable and join them in a single string.
    String setFieldsSQL =
        setFieldsForNestedVars.entrySet().stream()
            .sorted(Comparator.comparing(p -> p.getKey().getAliasOrColumnName()))
            .map(
                setField ->
                    new ST("<updateFieldSQL> = <selectFieldSQL>}")
                        .add("updateFieldSQL", setField.getKey().renderSQL(platform))
                        .add("selectFieldSQL", setField.getValue().renderSQL(platform))
                        .render())
            .collect(Collectors.joining(", "));

    return new ST(
            "UPDATE <updateTableSQL> SET <setFieldsSQL> FROM <selectTableSQL> WHERE <updateJoinFieldSQL> = <selectJoinField>")
        .add("updateTableSQL", updateTable.renderSQL(platform))
        .add("setFieldsSQL", setFieldsSQL)
        .add("selectTableSQL", nestedTableVar.renderSQL(platform))
        .add("updateJoinFieldSQL", updateJoinField.renderSQL(platform))
        .add("selectJoinField", selectJoinFieldForNestedVar.renderSQL(platform))
        .render();
  }
}
