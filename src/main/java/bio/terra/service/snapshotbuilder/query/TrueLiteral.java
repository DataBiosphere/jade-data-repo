package bio.terra.service.snapshotbuilder.query;

public class TrueLiteral implements SqlExpression {

  @Override
  public String renderSQL(SqlRenderContext context) {
    // In T-SQL, TRUE is not a keyword, so we need to use 1 instead. The JDBC API converts
    // the 1 to a boolean TRUE when the query result is processed.
    return context.getPlatform().choose("TRUE", "1");
  }
}
