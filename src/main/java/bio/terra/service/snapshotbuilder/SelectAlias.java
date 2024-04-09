package bio.terra.service.snapshotbuilder;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;

/** Wrap a field variable with an alias for use in a SELECT. */
public class SelectAlias implements SelectExpression {

  private final FieldVariable fieldVariable;
  private final String alias;

  public SelectAlias(FieldVariable fieldVariable, String alias) {
    this.alias = alias;
    this.fieldVariable = fieldVariable;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "%s AS %s".formatted(fieldVariable.renderSQL(context), alias);
  }
}
