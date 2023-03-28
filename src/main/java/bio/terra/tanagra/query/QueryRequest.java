package bio.terra.tanagra.query;

/** The request for a query to execute against a database backend. */
public class QueryRequest {
  // TODO: add parametrized arguments in SQL string
  private final String sql;
  private final ColumnHeaderSchema columnHeaderSchema;

  public QueryRequest(String sql, ColumnHeaderSchema columnHeaderSchema) {
    this.sql = sql;
    this.columnHeaderSchema = columnHeaderSchema;
  }

  public String getSql() {
    return sql;
  }

  public ColumnHeaderSchema getColumnHeaderSchema() {
    return columnHeaderSchema;
  }
}
