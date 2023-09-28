package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.List;
import java.util.stream.Collectors;

public class UnionQuery implements SQLExpression {
  private final List<Query> subQueries;

  public UnionQuery(List<Query> subQueries) {
    this.subQueries = subQueries;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    if (subQueries == null || subQueries.isEmpty()) {
      throw new SystemException("Union query must have at least one sub query");
    }

    return subQueries.stream()
        .map(sq -> sq.renderSQL(platform))
        .collect(Collectors.joining(" UNION ALL "));
  }
}
