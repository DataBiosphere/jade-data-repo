package bio.terra.service.snapshotbuilder.query;

public interface SqlExpression {
  String renderSQL(SqlRenderContext context);
}
