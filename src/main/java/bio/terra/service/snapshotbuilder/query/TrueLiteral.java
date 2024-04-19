package bio.terra.service.snapshotbuilder.query;

public class TrueLiteral implements SqlExpression {

  @Override
  public String renderSQL(SqlRenderContext context) {
    return context.getPlatform().choose("true", "1");
  }
}
