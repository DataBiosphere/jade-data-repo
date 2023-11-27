package bio.terra.service.snapshotbuilder.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class FieldVariable implements SqlExpression {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldVariable.class);
  private final FieldPointer fieldPointer;
  private final TableVariable tableVariable;
  private final String alias;

  private final boolean isDistinct;

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable) {
    this(fieldPointer, tableVariable, null, false);
  }

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable, String alias) {
    this(fieldPointer, tableVariable, alias, false);
  }

  public FieldVariable(
      FieldPointer fieldPointer, TableVariable tableVariable, String alias, boolean isDistinct) {
    this.fieldPointer = fieldPointer;
    this.tableVariable = tableVariable;
    this.alias = alias;
    this.isDistinct = isDistinct;
  }

  @Override
  public String renderSQL() {
    return renderSQL(true, true);
  }

  public String renderSqlForOrderOrGroupBy(boolean includedInSelect) {
    if (includedInSelect) {
      if (alias == null) {
        String sql = renderSQL(false, true);
        LOGGER.warn(
            "ORDER or GROUP BY clause is also included in SELECT but has no alias: {}", sql);
        return sql;
      }
      return alias;
    }
    return renderSQL(false, true);
  }

  public String renderSqlForWhere() {
    return renderSQL(false, true);
  }

  private String renderSQL(boolean useAlias, boolean useFunctionWrapper) {

    String sql =
        "%s%s.%s"
            .formatted(
                isDistinct ? "DISTINCT " : "",
                tableVariable.getAlias(),
                fieldPointer.getColumnName());

    if (fieldPointer.isForeignKey()) {
      throw new UnsupportedOperationException("TODO: implement embedded selects " + sql);
    }

    if (fieldPointer.hasSqlFunctionWrapper() && useFunctionWrapper) {
      String sqlFunctionWrapper = fieldPointer.getSqlFunctionWrapper();
      LOGGER.debug("Found sql function wrapper: {}", sqlFunctionWrapper);
      final String substitutionVar = "<fieldSql>";
      if (sqlFunctionWrapper.contains(substitutionVar)) {
        return new ST(sqlFunctionWrapper).add("fieldSql", sql).render();
      }
      return sqlFunctionWrapper + "(" + sql + ")";
    }

    if (alias != null && useAlias) {
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