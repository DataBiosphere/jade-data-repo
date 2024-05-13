package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import jakarta.annotation.Nullable;
import org.stringtemplate.v4.ST;

public final class SubQueryVariable implements SqlExpression {
  private final SubQueryPointer subQueryPointer;
  private final String joinField;
  private final FieldVariable joinFieldOnParent;
  private final boolean isLeftJoin;

  private SubQueryVariable(SubQueryPointer subQueryPointer,
                       @Nullable String joinField,
                       @Nullable FieldVariable joinFieldOnParent,
                       boolean isLeftJoin) {
    this.subQueryPointer = subQueryPointer;
    this.joinField = joinField;
    this.joinFieldOnParent = joinFieldOnParent;
    this.isLeftJoin = isLeftJoin;
  }

    public static SubQueryVariable forLeftJoined(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(tablePointer, joinField, joinFieldOnParent, true);
  }

  public static SubQueryVariable forLeftJoined(
      SubQueryPointer subQueryPointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(subQueryPointer, joinField, joinFieldOnParent, true);
  }

  public String getTableName() {
    return 'hi';
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    String sql = subQueryPointer.renderSQL(context);
//    String alias = context.getAlias(this);
    String alias = "blah";

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

  public boolean isPrimary() {
    return joinField == null;
  }
}
