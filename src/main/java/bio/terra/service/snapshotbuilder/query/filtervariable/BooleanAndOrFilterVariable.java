package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import java.util.stream.Collectors;

public record BooleanAndOrFilterVariable(
    BooleanAndOrFilterVariable.LogicalOperator operator, List<FilterVariable> subFilters)
    implements FilterVariable {

  @Override
  public String renderSQL() {
    return subFilters.stream()
        .map(SqlExpression::renderSQL)
        .collect(Collectors.joining(" " + operator.renderSQL() + " ", "(", ")"));
  }

  @Override
  public List<TableVariable> getTables() {
    return subFilters.stream().map(FilterVariable::getTables).flatMap(List::stream).toList();
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
