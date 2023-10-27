package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public class FieldPointer {
  private static final String ALL_FIELDS_COLUMN_NAME = "*";

  private final TablePointer tablePointer;
  private final String columnName;
  private final TablePointer foreignTablePointer;
  private final String foreignKeyColumnName;
  private final String foreignColumnName;
  private boolean joinCanBeEmpty;
  private final String sqlFunctionWrapper;

  private FieldPointer(
      TablePointer tablePointer,
      String columnName,
      TablePointer foreignTablePointer,
      String foreignKeyColumnName,
      String foreignColumnName,
      boolean joinCanBeEmpty,
      String sqlFunctionWrapper) {
    this.tablePointer = tablePointer;
    this.columnName = columnName;
    this.foreignTablePointer = foreignTablePointer;
    this.foreignKeyColumnName = foreignKeyColumnName;
    this.foreignColumnName = foreignColumnName;
    this.joinCanBeEmpty = joinCanBeEmpty;
    this.sqlFunctionWrapper = sqlFunctionWrapper;
  }

  public FieldPointer(TablePointer tablePointer, String columnName, String sqlFunctionWrapper) {
    this(tablePointer, columnName, null, null, null, false, sqlFunctionWrapper);
  }

  public FieldPointer(TablePointer tablePointer, String columnName) {
    this(tablePointer, columnName, null);
  }

  public static FieldPointer allFields(TablePointer tablePointer) {
    return new FieldPointer(tablePointer, ALL_FIELDS_COLUMN_NAME);
  }

  public static FieldPointer foreignColumn(
      TablePointer foreignTablePointer, String foreignColumnName) {
    return new FieldPointer(null, null, foreignTablePointer, null, foreignColumnName, false, null);
  }

  public FieldVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tableVariables) {
    return buildVariable(primaryTable, tableVariables, null);
  }

  public FieldVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tableVariables, String alias) {
    if (isForeignKey()) {
      FieldVariable primaryTableColumn =
          new FieldVariable(new FieldPointer(tablePointer, columnName), primaryTable);
      TableVariable foreignTable =
          joinCanBeEmpty
              ? TableVariable.forLeftJoined(
                  foreignTablePointer, foreignKeyColumnName, primaryTableColumn)
              : TableVariable.forJoined(
                  foreignTablePointer, foreignKeyColumnName, primaryTableColumn);
      // TODO: Check if there is already a table variable with the same JOIN criteria, so we don't
      // JOIN the same table for each field we need from it.
      tableVariables.add(foreignTable);
      return new FieldVariable(
          new FieldPointer(foreignTablePointer, foreignColumnName, sqlFunctionWrapper),
          foreignTable,
          alias);
    } else {
      return new FieldVariable(this, primaryTable, alias);
    }
  }

  public boolean isForeignKey() {
    return foreignTablePointer != null;
  }

  public String getColumnName() {
    return columnName;
  }

  public TablePointer getForeignTablePointer() {
    return foreignTablePointer;
  }

  public String getForeignKeyColumnName() {
    return foreignKeyColumnName;
  }

  public String getForeignColumnName() {
    return foreignColumnName;
  }

  public FieldPointer setJoinCanBeEmpty(boolean joinCanBeEmpty) {
    this.joinCanBeEmpty = joinCanBeEmpty;
    return this;
  }

  public boolean hasSqlFunctionWrapper() {
    return sqlFunctionWrapper != null;
  }

  public String getSqlFunctionWrapper() {
    return sqlFunctionWrapper;
  }

  public TablePointer getTablePointer() {
    return tablePointer;
  }
}
