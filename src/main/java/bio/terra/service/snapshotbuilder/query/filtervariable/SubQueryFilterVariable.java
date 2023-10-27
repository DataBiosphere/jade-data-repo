package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SQLExpression;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;
import org.stringtemplate.v4.ST;

public class SubQueryFilterVariable implements FilterVariable {
  private static final String TEMPLATE = "<fieldVariable> <operator> (<subQuery>)";

  private final FieldVariable fieldVariable;
  private final Operator operator;
  private final Query subQuery;

  public SubQueryFilterVariable(FieldVariable fieldVariable, Operator operator, Query subQuery) {
    this.fieldVariable = fieldVariable;
    this.operator = operator;
    this.subQuery = subQuery;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return new ST(TEMPLATE)
        .add("operator", operator.renderSQL(platform))
        .add("subQuery", subQuery.renderSQL(platform))
        .add("fieldVariable", fieldVariable.renderSqlForWhere())
        .render();
  }

  public enum Operator implements SQLExpression {
    IN("IN"),
    NOT_IN("NOT IN");

    private final String sql;

    Operator(String sql) {
      this.sql = sql;
    }

    @Override
    public String renderSQL(SqlPlatform platform) {
      return sql;
    }
  }
}
