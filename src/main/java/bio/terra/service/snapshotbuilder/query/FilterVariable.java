package bio.terra.service.snapshotbuilder.query;

public interface FilterVariable extends SqlExpression {
  static FilterVariable alwaysTrueFilter() {
    return context -> "1=1";
  }
}
