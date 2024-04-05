package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;

public class ExistsExpression implements SelectExpression {

  private final String alias;
  private final Query subQuery;

  public ExistsExpression(Query subQuery, String alias) {
    this.subQuery = subQuery;
    this.alias = alias;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "EXISTS (" + subQuery.renderSQL(context) + ") AS " + alias;
  }
}
