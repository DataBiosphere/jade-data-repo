package bio.terra.tanagra.query;

public enum OrderByDirection implements SQLExpression {
  ASCENDING("ASC"),
  DESCENDING("DESC");

  private final String sql;

  OrderByDirection(String sql) {
    this.sql = sql;
  }

  @Override
  public String renderSQL() {
    return sql;
  }
}
