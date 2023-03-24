package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.SQLExpression;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;

public class BinaryFilterVariable extends FilterVariable {
  private static final String SUBSTITUTION_TEMPLATE = "${fieldVariable} ${operator} ${value}";

  private final FieldVariable fieldVariable;
  private final BinaryOperator operator;
  private final Literal value;

  public BinaryFilterVariable(FieldVariable fieldVariable, BinaryOperator operator, Literal value) {
    this.fieldVariable = fieldVariable;
    this.operator = operator;
    this.value = value;
  }

  @Override
  protected String getSubstitutionTemplate() {
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("operator", operator.renderSQL())
            .put("value", value.renderSQL())
            .build();
    return StringSubstitutor.replace(SUBSTITUTION_TEMPLATE, params);
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return List.of(fieldVariable);
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

    private String sql;

    BinaryOperator(String sql) {
      this.sql = sql;
    }

    @Override
    public String renderSQL() {
      return sql;
    }
  }
}
