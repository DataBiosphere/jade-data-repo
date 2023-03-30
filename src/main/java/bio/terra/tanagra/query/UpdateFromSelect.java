package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import bio.terra.tanagra.exception.SystemException;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

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
  }

  @Override
  public String renderSQL(CloudPlatform platform) {
    // Check that the select join field is part of the query.
    if (!selectQuery.getSelect().contains(selectJoinField)) {
      throw new SystemException("Select join field is not part of the query selected fields");
    }

    // Build a table variable for a nested query.
    List<TableVariable> tableVars = new ArrayList<>();
    TablePointer nestedTable =
        TablePointer.fromRawSql(
            selectQuery.renderSQL(platform),
            selectQuery.getPrimaryTable().getTablePointer().getDataPointer());
    TableVariable nestedTableVar = TableVariable.forPrimary(nestedTable);
    tableVars.add(nestedTableVar);

    // Update the select set fields to point to the nested query table.
    Map<FieldVariable, FieldVariable> setFieldsForNestedVars = new HashMap<>();
    setFields.entrySet().stream()
        .forEach(
            setField -> {
              FieldVariable updateSetField = setField.getKey();
              FieldVariable selectSetField = setField.getValue();

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
                setField -> {
                  String template = "${updateFieldSQL} = ${selectFieldSQL}";
                  Map<String, String> params =
                      ImmutableMap.<String, String>builder()
                          .put("updateFieldSQL", setField.getKey().renderSQL(platform))
                          .put("selectFieldSQL", setField.getValue().renderSQL(platform))
                          .build();
                  return StringSubstitutor.replace(template, params);
                })
            .collect(Collectors.joining(", "));

    String template =
        "UPDATE ${updateTableSQL} SET ${setFieldsSQL} FROM ${selectTableSQL} WHERE ${updateJoinFieldSQL} = ${selectJoinField}";
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("updateTableSQL", updateTable.renderSQL(platform))
            .put("setFieldsSQL", setFieldsSQL)
            .put("selectTableSQL", nestedTableVar.renderSQL(platform))
            .put("updateJoinFieldSQL", updateJoinField.renderSQL(platform))
            .put("selectJoinField", selectJoinFieldForNestedVar.renderSQL(platform))
            .build();
    return StringSubstitutor.replace(template, params);
  }
}
