package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import java.util.List;
import java.util.stream.Collectors;

public record UnionQuery(List<Query> subQueries) implements SqlExpression {
  public UnionQuery {
    if (subQueries == null || subQueries.isEmpty()) {
      throw new IllegalArgumentException("Union query must have at least one sub query");
    }
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    return subQueries.stream()
        .map((query -> query.renderSQL(platform)))
        .collect(Collectors.joining(" UNION "));
  }
}
