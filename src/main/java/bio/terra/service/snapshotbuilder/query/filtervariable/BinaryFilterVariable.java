package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import org.stringtemplate.v4.ST;

public record BinaryFilterVariable(
    FieldVariable fieldVariable, BinaryFilterVariable.BinaryOperator operator, SqlExpression value)
    implements FilterVariable {
  private static final String SUBSTITUTION_TEMPLATE = "<fieldVariable> <operator> <value>";

  @Override
  public String renderSQL(SqlRenderContext context) {
    return new ST(SUBSTITUTION_TEMPLATE)
        .add("operator", operator.renderSQL(context))
        .add("value", value.renderSQL(context))
        .add("fieldVariable", fieldVariable.renderSQL(context))
        .render();
  }

  public enum BinaryOperator implements SqlExpression {
    EQUALS("="),
    NOT_EQUALS("!="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN_OR_EQUAL(">="),
    IS("IS"),
    IS_NOT("IS NOT");

    private final String sql;

    BinaryOperator(String sql) {
      this.sql = sql;
    }

    @Override
    public String renderSQL(SqlRenderContext context) {
      return sql;
    }
  }

  public static BinaryFilterVariable equals(FieldVariable fieldVariable, SqlExpression value) {
    return new BinaryFilterVariable(fieldVariable, BinaryOperator.EQUALS, value);
  }

  public static BinaryFilterVariable notEquals(FieldVariable fieldVariable, SqlExpression value) {
    return new BinaryFilterVariable(fieldVariable, BinaryOperator.NOT_EQUALS, value);
  }

  public static BinaryFilterVariable notNull(FieldVariable fieldVariable) {
    return new BinaryFilterVariable(fieldVariable, BinaryOperator.IS_NOT, new Literal());
  }
}
