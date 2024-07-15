package bio.terra.service.snapshotbuilder.query;

public interface SourcePointer extends SqlExpression {
  String getSourceName();
}
