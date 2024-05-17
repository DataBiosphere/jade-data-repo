package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public class FieldPointer {
  private static final String ALL_FIELDS_COLUMN_NAME = "*";

  private final SourcePointer sourcePointer;
  private final String columnName;
  private final SourcePointer foreignSourcePointer;
  private final String foreignKeyColumnName;
  private final String foreignColumnName;
  private final boolean joinCanBeEmpty;
  private final String sqlFunctionWrapper;

  private final String comparator;

  private FieldPointer(
      SourcePointer sourcePointer,
      String columnName,
      SourcePointer foreignSourcePointer,
      String foreignKeyColumnName,
      String foreignColumnName,
      boolean joinCanBeEmpty,
      String sqlFunctionWrapper,
      String comparator) {
    this.sourcePointer = sourcePointer;
    this.columnName = columnName;
    this.foreignSourcePointer = foreignSourcePointer;
    this.foreignKeyColumnName = foreignKeyColumnName;
    this.foreignColumnName = foreignColumnName;
    this.joinCanBeEmpty = joinCanBeEmpty;
    this.sqlFunctionWrapper = sqlFunctionWrapper;
    this.comparator = comparator;
  }

  public FieldPointer(
      SourcePointer sourcePointer,
      String columnName,
      String sqlFunctionWrapper,
      String comparator) {
    this(sourcePointer, columnName, null, null, null, false, sqlFunctionWrapper, comparator);
  }

  public FieldPointer(SourcePointer sourcePointer, String columnName, String sqlFunctionWrapper) {
    this(sourcePointer, columnName, null, null, null, false, sqlFunctionWrapper, null);
  }

  public FieldPointer(SourcePointer sourcePointer, String columnName) {
    this(sourcePointer, columnName, null);
  }

  public static FieldPointer allFields(SourcePointer sourcePointer) {
    return new FieldPointer(sourcePointer, ALL_FIELDS_COLUMN_NAME);
  }

  public static FieldPointer foreignColumn(
      SourcePointer foreignSourcePointer, String foreignColumnName) {
    return new FieldPointer(
        null, null, foreignSourcePointer, null, foreignColumnName, false, null, null);
  }

  public FieldVariable buildVariable(
      SourceVariable primaryTable, List<SourceVariable> sourceVariables) {
    return buildVariable(primaryTable, sourceVariables, null);
  }

  public FieldVariable buildVariable(
      SourceVariable primaryTable, List<SourceVariable> sourceVariables, String alias) {
    if (isForeignKey()) {
      FieldVariable primaryTableColumn =
          new FieldVariable(new FieldPointer(sourcePointer, columnName), primaryTable);
      SourceVariable foreignTable =
          joinCanBeEmpty
              ? SourceVariable.forLeftJoined(
                  foreignSourcePointer, foreignKeyColumnName, primaryTableColumn)
              : SourceVariable.forJoined(
                  foreignSourcePointer, foreignKeyColumnName, primaryTableColumn);
      // TODO: Check if there is already a table variable with the same JOIN criteria, so we don't
      // JOIN the same table for each field we need from it.
      sourceVariables.add(foreignTable);
      return new FieldVariable(
          new FieldPointer(foreignSourcePointer, foreignColumnName, sqlFunctionWrapper, comparator),
          foreignTable,
          alias);
    } else {
      return new FieldVariable(this, primaryTable, alias);
    }
  }

  public boolean isForeignKey() {
    return foreignSourcePointer != null;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getForeignColumnName() {
    return foreignColumnName;
  }

  public boolean hasSqlFunctionWrapper() {
    return sqlFunctionWrapper != null;
  }

  public String getSqlFunctionWrapper() {
    return sqlFunctionWrapper;
  }

  public boolean hasComparator() {
    return comparator != null;
  }

  public String getComparator() {
    return comparator;
  }
}
