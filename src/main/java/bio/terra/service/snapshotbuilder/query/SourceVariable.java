package bio.terra.service.snapshotbuilder.query;

import jakarta.annotation.Nullable;
import org.stringtemplate.v4.ST;

public final class SourceVariable implements SqlExpression {
  private final SourcePointer sourcePointer;
  private final String joinField;
  private final FieldVariable joinFieldOnParent;
  private final boolean isLeftJoin;

  private SourceVariable(
      SourcePointer sourcePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    this.sourcePointer = sourcePointer;
    this.joinField = joinField;
    this.joinFieldOnParent = joinFieldOnParent;
    this.isLeftJoin = isLeftJoin;
  }

  public static SourceVariable forPrimary(SourcePointer sourcePointer) {
    return new SourceVariable(sourcePointer, null, null, false);
  }

  public static SourceVariable forJoined(
      SourcePointer sourcePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(sourcePointer, joinField, joinFieldOnParent, false);
  }

  public static SourceVariable forLeftJoined(
      SourcePointer sourcePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(sourcePointer, joinField, joinFieldOnParent, true);
  }

  private static SourceVariable forJoined(
      SourcePointer sourcePointer,
      String joinField,
      FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    return new SourceVariable(sourcePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  public FieldVariable makeFieldVariable(String fieldName) {
    FieldPointer fieldPointer = new FieldPointer(sourcePointer, fieldName);
    return new FieldVariable(fieldPointer, this);
  }

  public FieldVariable makeFieldVariable(
      String fieldName, String sqlFunctionWrapper, String alias, boolean isDistinct) {
    FieldPointer fieldPointer = new FieldPointer(sourcePointer, fieldName, sqlFunctionWrapper);
    return new FieldVariable(fieldPointer, this, alias, isDistinct);
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    String sql = sourcePointer.renderSQL(context);
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

  public SourcePointer getSourcePointer() {
    return sourcePointer;
  }

  public boolean isPrimary() {
    return joinField == null;
  }
}
