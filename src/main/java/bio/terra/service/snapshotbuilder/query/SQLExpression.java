package bio.terra.service.snapshotbuilder.query;

public interface SQLExpression {
  String renderSQL(SqlPlatform platform);

  // maybe add static no-op sqlexpression to avoid null checks
}
