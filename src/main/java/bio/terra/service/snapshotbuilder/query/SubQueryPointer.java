package bio.terra.service.snapshotbuilder.query;

public record SubQueryPointer(Query query, String subQueryName) implements SourcePointer {

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "(" + query.renderSQL(context) + ")";
  }

  @Override
  public String getSourceName() {
    return subQueryName;
  }
}
