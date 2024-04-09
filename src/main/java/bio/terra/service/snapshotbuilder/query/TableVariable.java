package bio.terra.service.snapshotbuilder.query;

import jakarta.annotation.Nullable;
import org.stringtemplate.v4.ST;

public final class TableVariable implements SqlExpression {
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
      String fieldName, String sqlFunctionWrapper, String alias, boolean isDistinct) {
    FieldPointer fieldPointer = new FieldPointer(tablePointer, fieldName, sqlFunctionWrapper);
    return new FieldVariable(fieldPointer, this, alias, isDistinct);
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    String sql = tablePointer.renderSQL(context);
    String alias = context.getAlias(this);

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
              .add("joinFieldOnParent", joinFieldOnParent.renderSQL(context))
              .render();
    }
    return sql;
  }

  /** Default table alias is the first letter of the table name. */
  public String getDefaultAlias() {
    String tableName = tablePointer.tableName();
    return tableName != null ? tableName.toLowerCase().substring(0, 1) : "x";
  }

  public TablePointer getTablePointer() {
    return tablePointer;
  }

  public boolean isPrimary() {
    return joinField == null;
  }
}
