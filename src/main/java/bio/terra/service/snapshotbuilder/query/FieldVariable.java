package bio.terra.service.snapshotbuilder.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class FieldVariable implements SelectExpression {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldVariable.class);
  private final FieldPointer fieldPointer;
  private final SourceVariable sourceVariable;
  private final String alias;

  private final boolean isDistinct;

  public FieldVariable(FieldPointer fieldPointer, SourceVariable sourceVariable) {
    this(fieldPointer, sourceVariable, null, false);
  }

  public FieldVariable(FieldPointer fieldPointer, SourceVariable sourceVariable, String alias) {
    this(fieldPointer, sourceVariable, alias, false);
  }

  public FieldVariable(
      FieldPointer fieldPointer, SourceVariable sourceVariable, String alias, boolean isDistinct) {
    this.fieldPointer = fieldPointer;
    this.sourceVariable = sourceVariable;
    this.alias = alias;
    this.isDistinct = isDistinct;
  }

  public SourceVariable getSourceVariable() {
    return sourceVariable;
  }

  public String renderSqlForOrderOrGroupBy(boolean includedInSelect, SqlRenderContext context) {
    if (includedInSelect) {
      if (alias == null) {
        String sql = renderSQL(context);
        LOGGER.warn(
            "ORDER or GROUP BY clause is also included in SELECT but has no alias: {}", sql);
        return sql;
      }
      return alias;
    }
    return renderSQL(context);
  }

  @Override
  public String renderSQL(SqlRenderContext context) {

    String sql =
        "%s%s.%s"
            .formatted(
                isDistinct ? "DISTINCT " : "",
                context.getAlias(sourceVariable),
                fieldPointer.getColumnName());

    if (fieldPointer.isForeignKey()) {
      throw new UnsupportedOperationException("TODO: implement embedded selects " + sql);
    }

    if (fieldPointer.hasSqlFunctionWrapper()) {
      String sqlFunctionWrapper = fieldPointer.getSqlFunctionWrapper();
      LOGGER.debug("Found sql function wrapper: {}", sqlFunctionWrapper);
      final String substitutionVar = "<fieldSql>";
      if (sqlFunctionWrapper.contains(substitutionVar)) {
        sql = new ST(sqlFunctionWrapper).add("fieldSql", sql).render();
      } else {
        sql = sqlFunctionWrapper + "(" + sql + ")";
      }
    }

    if (fieldPointer.hasHasCountClause() && context.getPlatform().isGcp()) {
      sql = sql + " " + fieldPointer.getHasCountClause();
    }

    if (alias != null) {
      return "%s AS %s".formatted(sql, alias);
    }

    return sql;
  }

  public String getAlias() {
    return alias == null ? "" : alias;
  }

  public String getAliasOrColumnName() {
    if (alias != null) {
      return alias;
    }
    if (fieldPointer.isForeignKey()) {
      return fieldPointer.getForeignColumnName();
    }
    return fieldPointer.getColumnName();
  }

  public FieldPointer getFieldPointer() {
    return fieldPointer;
  }
}
