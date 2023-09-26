package bio.terra.tanagra.query;

public interface SQLExpression {
  String renderSQL(SqlPlatform platform);

  default String renderSQL() {
    return renderSQL(SqlPlatform.BIGQUERY);
  }
}
