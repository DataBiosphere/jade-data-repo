package bio.terra.tanagra.query;

public interface SQLExpression {
  String renderSQL(SqlPlatform platform);
}
