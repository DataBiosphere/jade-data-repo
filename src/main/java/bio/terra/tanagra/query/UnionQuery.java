package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.List;
import java.util.stream.Collectors;

public class UnionQuery implements SQLExpression {
  private final List<Query> subqueries;

  public UnionQuery(List<Query> subqueries) {
    this.subqueries = subqueries;
  }

  @Override
  public String renderSQL() {
    if (subqueries == null || subqueries.isEmpty()) {
      throw new SystemException("Union query must have at least one sub query");
    }

    return subqueries.stream().map(sq -> sq.renderSQL()).collect(Collectors.joining(" UNION ALL "));
  }
}
