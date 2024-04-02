package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.stringtemplate.v4.ST;

public final class TableVariable implements SqlExpression {
  private String alias;
  private final TablePointer tablePointer;
  private final String joinField;
  private final FieldVariable joinFieldOnParent;
  private final boolean isLeftJoin;

  private TableVariable(
      TablePointer tablePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    this.tablePointer = tablePointer;
    this.joinField = joinField;
    this.joinFieldOnParent = joinFieldOnParent;
    this.isLeftJoin = isLeftJoin;
  }

  public static TableVariable forPrimary(TablePointer tablePointer) {
    return new TableVariable(tablePointer, null, null, false);
  }

  public static TableVariable forJoined(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(tablePointer, joinField, joinFieldOnParent, false);
  }

  public static TableVariable forLeftJoined(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(tablePointer, joinField, joinFieldOnParent, true);
  }

  private static TableVariable forJoined(
      TablePointer tablePointer,
      String joinField,
      FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    return new TableVariable(tablePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  public FieldVariable makeFieldVariable(String fieldName) {
    FieldPointer fieldPointer = new FieldPointer(tablePointer, fieldName);
    return new FieldVariable(fieldPointer, this);
  }

  public FieldVariable makeFieldVariable(
      String fieldName, String SQLFunctionWrapper, String alias, boolean isDistinct) {
    FieldPointer fieldPointer = new FieldPointer(tablePointer, fieldName, SQLFunctionWrapper);
    return new FieldVariable(fieldPointer, this, alias, isDistinct);
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    String sql = tablePointer.renderSQL(platform);

    if (alias != null) {
      sql = new ST("<sql> AS <tableAlias>").add("sql", sql).add("tableAlias", alias).render();
    }

    if (joinField != null && joinFieldOnParent != null && alias != null) {
      sql =
          new ST("<joinType> <tableReference> ON <tableAlias>.<joinField> = <joinFieldOnParent>")
              .add("joinType", isLeftJoin ? "LEFT JOIN" : "JOIN")
              .add("tableReference", sql)
              .add("tableAlias", alias)
              .add("joinField", joinField)
              .add("joinFieldOnParent", joinFieldOnParent.renderSQL(platform))
              .render();
    }
    return sql;
  }

  /**
   * Iterate through all the {@link TableVariable}s and generate a unique alias for each one. Start
   * with the default alias (= first letter of the table name) and if that's taken, append
   * successively higher integers until we find one that doesn't conflict with any other table
   * aliases.
   */
  public static void generateAliases(List<TableVariable> tableVariables) {
    Map<String, TableVariable> aliases = new HashMap<>();
    for (TableVariable tableVariable : tableVariables) {
      String defaultAlias = tableVariable.getDefaultAlias();
      String alias = defaultAlias;
      int suffix = 0;
      while (aliases.containsKey(alias)) {
        alias = defaultAlias + suffix++;
      }
      tableVariable.setAlias(alias);
      aliases.put(alias, tableVariable);
    }
  }

  /** Default table alias is the first letter of the table name. */
  private String getDefaultAlias() {
    String tableName = tablePointer.tableName();
    return tableName != null ? tableName.toLowerCase().substring(0, 1) : "x";
  }

  public String getAlias() {
    return alias;
  }

  private void setAlias(String alias) {
    this.alias = alias;
  }

  public TablePointer getTablePointer() {
    return tablePointer;
  }

  public boolean isPrimary() {
    return joinField == null;
  }
}
