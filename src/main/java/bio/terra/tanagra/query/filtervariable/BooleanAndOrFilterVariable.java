package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;
import java.util.List;
import java.util.stream.Collectors;

public class BooleanAndOrFilterVariable implements FilterVariable {
  private final LogicalOperator operator;
  private final List<FilterVariable> subFilters;

  public BooleanAndOrFilterVariable(LogicalOperator operator, List<FilterVariable> subFilters) {
    this.operator = operator;
    this.subFilters = subFilters;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return subFilters.stream()
        .map(sf -> sf.renderSQL(platform))
        .collect(Collectors.joining(" " + operator.renderSQL(platform) + " ", "(", ")"));
  }

  public enum LogicalOperator implements SQLExpression {
    AND,
    OR;

    @Override
    public String renderSQL(SqlPlatform platform) {
      return name();
    }
  }
}
