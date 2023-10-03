package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;
import org.stringtemplate.v4.ST;

public class BinaryFilterVariable implements FilterVariable {
  private static final String SUBSTITUTION_TEMPLATE = "<fieldVariable> <operator> <value>";

  private final FieldVariable fieldVariable;
  private final BinaryOperator operator;
  private final Literal value;

  public BinaryFilterVariable(FieldVariable fieldVariable, BinaryOperator operator, Literal value) {
    this.fieldVariable = fieldVariable;
    this.operator = operator;
    this.value = value;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return new ST(SUBSTITUTION_TEMPLATE)
        .add("operator", operator.renderSQL(platform))
        .add("value", value.renderSQL(platform))
        .add("fieldVariable", fieldVariable.renderSqlForWhere())
        .render();
  }

  public enum BinaryOperator implements SQLExpression {
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
    public String renderSQL(SqlPlatform platform) {
      return sql;
    }
  }
}
