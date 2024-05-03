package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import java.util.List;
import java.util.stream.Collectors;

public record BooleanAndOrFilterVariable(
    BooleanAndOrFilterVariable.LogicalOperator operator, List<FilterVariable> subFilters)
    implements FilterVariable {

  @Override
  public String renderSQL(SqlRenderContext context) {
    return subFilters.isEmpty()
        ? "1=1"
        : subFilters.stream()
            .map(exp -> exp.renderSQL(context))
            .collect(Collectors.joining(" " + operator.renderSQL(context) + " ", "(", ")"));
  }

  public enum LogicalOperator implements SqlExpression {
    AND,
    OR;

    @Override
    public String renderSQL(SqlRenderContext context) {
      return name();
    }
  }

  public static BooleanAndOrFilterVariable and(FilterVariable... subFilters) {
    return new BooleanAndOrFilterVariable(LogicalOperator.AND, List.of(subFilters));
  }
}
