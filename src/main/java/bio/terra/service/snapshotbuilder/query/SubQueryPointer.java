package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public record SubQueryPointer(Query query) implements SqlExpression {

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "(" + query.renderSQL(context) + ")";
  }
}
