package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.SQLExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BooleanAndOrFilterVariable extends FilterVariable {
  private final LogicalOperator operator;
  private final List<FilterVariable> subFilters;

  public BooleanAndOrFilterVariable(LogicalOperator operator, List<FilterVariable> subFilters) {
    this.operator = operator;
    this.subFilters = subFilters;
  }

  @Override
  protected String getSubstitutionTemplate() {
    return null;
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    List<FieldVariable> fieldVars = new ArrayList<>();
    subFilters.stream().forEach(subFilter -> fieldVars.addAll(subFilter.getFieldVariables()));
    return fieldVars;
  }

  @Override
  public String renderSQL() {
    return "("
        + subFilters.stream()
            .map(sf -> sf.renderSQL())
            .collect(Collectors.joining(" " + operator.renderSQL() + " "))
        + ")";
  }

  public enum LogicalOperator implements SQLExpression {
    AND,
    OR;

    @Override
    public String renderSQL() {
      return name();
    }
  }
}
