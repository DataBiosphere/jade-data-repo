package bio.terra.service.snapshotbuilder;

import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;

/** Wrap a field variable with an alias for use in a SELECT. */
public class SelectAlias implements SelectExpression {

  private final SqlExpression expression;
  private final String alias;

  public SelectAlias(SqlExpression expression, String alias) {
    this.alias = alias;
    this.expression = expression;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "%s AS %s".formatted(expression.renderSQL(context), alias);
  }
}
