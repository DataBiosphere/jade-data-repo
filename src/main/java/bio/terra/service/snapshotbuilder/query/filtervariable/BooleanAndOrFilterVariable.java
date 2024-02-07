package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import java.util.List;
import java.util.stream.Collectors;

public record BooleanAndOrFilterVariable(
    BooleanAndOrFilterVariable.LogicalOperator operator, List<FilterVariable> subFilters)
    implements FilterVariable {

  @Override
  public String renderSQL() {
    return subFilters.isEmpty()
        ? "1=1"
        : subFilters.stream()
            .map(SqlExpression::renderSQL)
            .collect(Collectors.joining(" " + operator.renderSQL() + " ", "(", ")"));
  }

  public enum LogicalOperator implements SqlExpression {
    AND,
    OR;

    @Override
    public String renderSQL() {
      return name();
    }
  }
}
