package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import java.util.ArrayList;
import java.util.List;
import org.stringtemplate.v4.ST;

public final class SourceVariable implements SqlExpression {
  private final SourcePointer sourcePointer;
  private final List<FilterVariable> joinClauses = new ArrayList<>();
  private final boolean isLeftJoin;

  private SourceVariable(SourcePointer sourcePointer, boolean isLeftJoin) {
    this.sourcePointer = sourcePointer;
    this.isLeftJoin = isLeftJoin;
  }

  public static SourceVariable forPrimary(SourcePointer sourcePointer) {
    return new SourceVariable(sourcePointer, false);
  }

  public void addJoinClause(
      String joinField, Literal joinFieldOnParent, BinaryFilterVariable.BinaryOperator operator) {
    var joinClause =
        new BinaryFilterVariable(makeFieldVariable(joinField), operator, joinFieldOnParent);
    joinClauses.add(joinClause);
  }

  public void addJoinClause(
      String joinField,
      FieldVariable joinFieldOnParent,
      BinaryFilterVariable.BinaryOperator operator) {
    var joinClause =
        new BinaryFilterVariable(makeFieldVariable(joinField), operator, joinFieldOnParent);
    joinClauses.add(joinClause);
  }

  public void addJoinClause(String joinField, FieldVariable joinFieldOnParent) {
    addJoinClause(joinField, joinFieldOnParent, BinaryFilterVariable.BinaryOperator.EQUALS);
  }

  public static SourceVariable forJoined(
      SourcePointer sourcePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(sourcePointer, joinField, joinFieldOnParent, false);
  }

  public static SourceVariable forLeftJoined(
      SourcePointer sourcePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(sourcePointer, joinField, joinFieldOnParent, true);
  }

  public static SourceVariable forJoined(
      SourcePointer sourcePointer,
      String joinField,
      FieldVariable fieldVariable,
      boolean isLeftJoin) {
    var sourceVariable = new SourceVariable(sourcePointer, isLeftJoin);
    sourceVariable.addJoinClause(joinField, fieldVariable);
    return sourceVariable;
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
      sql = new ST("<sql> AS <alias>").add("sql", sql).add("alias", alias).render();
    }

    if (!joinClauses.isEmpty() && alias != null) {
      var joinClause =
          joinClauses.size() > 1
              ? new BooleanAndOrFilterVariable(
                  BooleanAndOrFilterVariable.LogicalOperator.AND, joinClauses)
              : joinClauses.get(0);

      sql =
          new ST("<joinType> <tableReference> ON <joinClause>")
              .add("joinType", isLeftJoin ? "LEFT JOIN" : "JOIN")
              .add("tableReference", sql)
              .add("joinClause", joinClause.renderSQL(context))
              .render();
    }
    return sql;
  }

  public SourcePointer getSourcePointer() {
    return sourcePointer;
  }

  public boolean isPrimary() {
    return joinClauses.isEmpty();
  }
}
