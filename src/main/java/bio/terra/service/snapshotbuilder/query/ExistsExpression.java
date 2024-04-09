package bio.terra.service.snapshotbuilder.query;

import org.stringtemplate.v4.ST;

public class ExistsExpression implements SelectExpression {

  static final String GCP_TEMPLATE = "EXISTS (<subQuery>) AS <alias>";
  static final String AZURE_TEMPLATE = "CASE WHEN EXISTS (<subQuery>) THEN 1 ELSE 0 END AS <alias>";

  private final String alias;
  private final Query subQuery;

  public ExistsExpression(Query subQuery, String alias) {
    this.subQuery = subQuery;
    this.alias = alias;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return new ST(context.getPlatform().choose(GCP_TEMPLATE, AZURE_TEMPLATE))
        .add("subQuery", subQuery.renderSQL(context))
        .add("alias", alias)
        .render();
  }
}
