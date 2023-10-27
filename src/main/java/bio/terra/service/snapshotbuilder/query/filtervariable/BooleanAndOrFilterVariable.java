package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SQLExpression;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;
import java.util.List;
import java.util.stream.Collectors;

public record BooleanAndOrFilterVariable(
    BooleanAndOrFilterVariable.LogicalOperator operator, List<FilterVariable> subFilters)
    implements FilterVariable {

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
