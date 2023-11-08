package bio.terra.service.snapshotbuilder.query;

public interface SqlExpression {
  String renderSQL();

  // maybe add static no-op sqlexpression to avoid null checks
}
