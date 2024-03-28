package bio.terra.service.snapshotbuilder.query;

public interface FilterVariable extends SqlExpression {
  static FilterVariable alwaysTrueFilter() {
    return platform -> "1=1";
  }
}
