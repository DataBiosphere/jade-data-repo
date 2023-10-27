package bio.terra.service.snapshotbuilder.query;

import java.util.List;
import java.util.stream.Collectors;

public record UnionQuery(List<Query> subQueries) implements SQLExpression {
  public UnionQuery {
    if (subQueries == null || subQueries.isEmpty()) {
      throw new IllegalArgumentException("Union query must have at least one sub query");
    }
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return subQueries.stream()
        .map(sq -> sq.renderSQL(platform))
        .collect(Collectors.joining(" UNION "));
  }
}
