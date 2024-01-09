package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.serialization.UFFieldPointer;
import com.google.common.base.Strings;
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

  private FieldPointer(Builder builder) {
    this.tablePointer = builder.tablePointer;
    this.columnName = builder.columnName;
    this.foreignTablePointer = builder.foreignTablePointer;
    this.foreignKeyColumnName = builder.foreignKeyColumnName;
    this.foreignColumnName = builder.foreignColumnName;
    this.joinCanBeEmpty = builder.joinCanBeEmpty;
    this.sqlFunctionWrapper = builder.sqlFunctionWrapper;
  }

  public static FieldPointer allFields(TablePointer tablePointer) {
    return new Builder().tablePointer(tablePointer).columnName(ALL_FIELDS_COLUMN_NAME).build();
  }

  public static FieldPointer fromSerialized(UFFieldPointer serialized, TablePointer tablePointer) {
    boolean foreignTableDefined = !Strings.isNullOrEmpty(serialized.getForeignTable());
    boolean foreignKeyColumnDefined = !Strings.isNullOrEmpty(serialized.getForeignKey());
    boolean foreignColumnDefined = !Strings.isNullOrEmpty(serialized.getForeignColumn());
    boolean allForeignKeyFieldsDefined =
        foreignTableDefined && foreignKeyColumnDefined && foreignColumnDefined;
    boolean noForeignKeyFieldsDefined =
        !foreignTableDefined && !foreignKeyColumnDefined && !foreignColumnDefined;

    if (noForeignKeyFieldsDefined) {
      return new Builder()
          .tablePointer(tablePointer)
          .columnName(serialized.getColumn())
          .sqlFunctionWrapper(serialized.getSqlFunctionWrapper())
          .build();
    } else if (allForeignKeyFieldsDefined) {
      // assume the foreign table is part of the same data pointer as the original table
      TablePointer foreignTablePointer =
          TablePointer.fromTableName(serialized.getForeignTable(), tablePointer.getDataPointer());
      return new Builder()
          .tablePointer(tablePointer)
          .columnName(serialized.getColumn())
          .foreignTablePointer(foreignTablePointer)
          .foreignKeyColumnName(serialized.getForeignKey())
          .foreignColumnName(serialized.getForeignColumn())
          .sqlFunctionWrapper(serialized.getSqlFunctionWrapper())
          .build();
    } else {
      throw new InvalidConfigException("Only some foreign key fields are defined");
    }
  }

  public FieldVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tableVariables) {
    return buildVariable(primaryTable, tableVariables, null);
  }

  public FieldVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tableVariables, String alias) {
    if (isForeignKey()) {
      FieldVariable primaryTableColumn =
          new FieldVariable(
              new Builder().tablePointer(tablePointer).columnName(columnName).build(),
              primaryTable);
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
          new Builder()
              .tablePointer(foreignTablePointer)
              .columnName(foreignColumnName)
              .sqlFunctionWrapper(sqlFunctionWrapper)
              .build(),
          foreignTable,
          alias);
    } else {
      return new FieldVariable(this, primaryTable, alias);
    }
  }

  public Builder toBuilder() {
    return new Builder()
        .tablePointer(tablePointer)
        .columnName(columnName)
        .foreignTablePointer(foreignTablePointer)
        .foreignKeyColumnName(foreignKeyColumnName)
        .foreignColumnName(foreignColumnName)
        .joinCanBeEmpty(joinCanBeEmpty)
        .sqlFunctionWrapper(sqlFunctionWrapper);
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

  public static class Builder {
    private TablePointer tablePointer;
    private String columnName;
    private TablePointer foreignTablePointer;
    private String foreignKeyColumnName;
    private String foreignColumnName;
    private boolean joinCanBeEmpty;
    private String sqlFunctionWrapper;

    public Builder tablePointer(TablePointer tablePointer) {
      this.tablePointer = tablePointer;
      return this;
    }

    public Builder columnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder foreignTablePointer(TablePointer foreignTablePointer) {
      this.foreignTablePointer = foreignTablePointer;
      return this;
    }

    public Builder foreignKeyColumnName(String foreignKeyColumnName) {
      this.foreignKeyColumnName = foreignKeyColumnName;
      return this;
    }

    public Builder foreignColumnName(String foreignColumnName) {
      this.foreignColumnName = foreignColumnName;
      return this;
    }

    public Builder joinCanBeEmpty(boolean joinCanBeEmpty) {
      this.joinCanBeEmpty = joinCanBeEmpty;
      return this;
    }

    public Builder sqlFunctionWrapper(String sqlFunctionWrapper) {
      this.sqlFunctionWrapper = sqlFunctionWrapper;
      return this;
    }

    /** Call the private constructor. */
    public FieldPointer build() {
      return new FieldPointer(this);
    }
  }
}
