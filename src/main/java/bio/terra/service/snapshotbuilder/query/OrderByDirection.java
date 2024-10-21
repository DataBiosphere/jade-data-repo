package bio.terra.service.snapshotbuilder.query;

public enum OrderByDirection implements SqlExpression {
  ASCENDING("ASC"),
  DESCENDING("DESC");

  private final String sql;

  OrderByDirection(String sql) {
    this.sql = sql;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return sql;
  }
}
