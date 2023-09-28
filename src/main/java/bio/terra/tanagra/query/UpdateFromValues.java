package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class UpdateFromValues implements SQLExpression {
  private final TableVariable updateTable;
  private final Map<FieldVariable, FieldVariable> setFields;
  private final Query selectQuery;
  private final FieldVariable updateJoinField;
  private final FieldVariable selectJoinField;
  private final Collection<RowResult> rows;

  public UpdateFromValues(
      TableVariable updateTable,
      Map<FieldVariable, FieldVariable> setFields,
      Query selectQuery,
      FieldVariable updateJoinField,
      FieldVariable selectJoinField,
      Collection<RowResult> rows) {
    this.updateTable = updateTable;
    this.setFields = setFields;
    this.selectQuery = selectQuery;
    this.updateJoinField = updateJoinField;
    this.selectJoinField = selectJoinField;
    this.rows = rows;
  }

  /*
  SELECT * FROM UNNEST([STRUCT<name STRING, age INT64>
    ('david',10), ('tom', 20), ('jon', 30)
  ])
   */

  private String renderLiteralData(TableVariable nestedTableVar, SqlPlatform platform) {
    RowResult firstRow = rows.iterator().next();

    List<FieldVariable> selectVars = new ArrayList<>(setFields.values());
    // FIXME: there should be a way to do this without hardcoding "id".
    selectVars.add(
        new FieldVariable(new FieldPointer.Builder().columnName("id").build(), nestedTableVar));

    String fields =
        selectVars.stream()
            .map(
                fieldVariable ->
                    fieldVariable.getFieldPointer().getColumnName()
                        + " "
                        + firstRow.get(fieldVariable.getAliasOrColumnName()).dataType())
            .collect(Collectors.joining(", "));

    String values =
        rows.stream()
            .map(
                rowResult ->
                    selectVars.stream()
                        .map(FieldVariable::getFieldPointer)
                        .map(FieldPointer::getColumnName)
                        .map(rowResult::get)
                        .flatMap(cellValue -> cellValue.getLiteral().stream())
                        .map(literal -> literal.renderSQL(platform))
                        .collect(Collectors.joining(",", "(", ")")))
            .collect(Collectors.joining(","));

    String template = "(SELECT * FROM UNNEST([STRUCT\\<<fields>\\> <values>])) AS <alias>";
    return new ST(template)
        .add("fields", fields)
        .add("values", values)
        .add("alias", selectQuery.getPrimaryTable().getAlias())
        .render();
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    // Check that the select join field is part of the query.
    if (!selectQuery.getSelect().contains(selectJoinField)) {
      throw new SystemException("Select join field is not part of the query selected fields");
    }

    // Build a table variable for a nested query.
    List<TableVariable> tableVars = new ArrayList<>();
    TablePointer nestedTable =
        TablePointer.fromRawSql(
            selectQuery.renderSQL(platform),
            selectQuery.getPrimaryTable().getTablePointer().dataPointer());
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
                    new ST("<updateFieldSQL> = <selectFieldSQL>")
                        .add("updateFieldSQL", setField.getKey().renderSQL(platform))
                        .add("selectFieldSQL", setField.getValue().renderSQL(platform))
                        .render())
            .collect(Collectors.joining(", "));

    return new ST(
            "UPDATE <updateTableSQL> SET <setFieldsSQL> FROM <selectTableSQL> WHERE <updateJoinFieldSQL> = <selectJoinField>}")
        .add("updateTableSQL", updateTable.renderSQL(platform))
        .add("setFieldsSQL", setFieldsSQL)
        .add("selectTableSQL", renderLiteralData(nestedTableVar, platform))
        .add("updateJoinFieldSQL", updateJoinField.renderSQL(platform))
        .add("selectJoinField", selectJoinFieldForNestedVar.renderSQL(platform))
        .render();
  }
}
