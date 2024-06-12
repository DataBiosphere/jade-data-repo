package bio.terra.service.snapshotbuilder.query;

public class SubstituteVariable implements SelectExpression {
  private final String searchText;

  public SubstituteVariable(String searchText) {
    this.searchText = searchText;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return context.getPlatform().choose("@" + searchText, ":" + searchText);
  }
}
