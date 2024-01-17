package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.stringtemplate.v4.ST;

public record BinaryFilterVariable(
    FieldVariable fieldVariable, BinaryFilterVariable.BinaryOperator operator, Literal value)
    implements FilterVariable {
  private static final String SUBSTITUTION_TEMPLATE = "<fieldVariable> <operator> <value>";

  @Override
  public String renderSQL() {
    return new ST(SUBSTITUTION_TEMPLATE)
        .add("operator", operator.renderSQL())
        .add("value", value.renderSQL())
        .add("fieldVariable", fieldVariable.renderSqlForWhere())
        .render();
  }

  @Override
  public List<TableVariable> getTables() {
    return List.of(fieldVariable.getTableVariable());
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
    public String renderSQL() {
      return sql;
    }
  }
}
