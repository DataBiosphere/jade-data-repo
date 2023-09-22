package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldVariable implements SQLExpression {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldVariable.class);
  private final FieldPointer fieldPointer;
  private final TableVariable tableVariable;
  private final String alias;

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable) {
    this(fieldPointer, tableVariable, null);
  }

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable, String alias) {
    this.fieldPointer = fieldPointer;
    this.tableVariable = tableVariable;
    this.alias = alias;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return renderSQL(true, true);
  }

  public String renderSqlForOrderBy() {
    return renderSQL(false, false);
  }

  public String renderSqlForWhere() {
    return renderSQL(false, true);
  }

  private String renderSQL(boolean useAlias, boolean useFunctionWrapper) {
    String sql = "%s.%s".formatted(tableVariable.getAlias(), fieldPointer.getColumnName());

    if (fieldPointer.isForeignKey()) {
      throw new SystemException("TODO: implement embedded selects " + sql);
    }

    if (fieldPointer.hasSqlFunctionWrapper() && useFunctionWrapper) {
      String sqlFunctionWrapper = fieldPointer.getSqlFunctionWrapper();
      LOGGER.debug("Found sql function wrapper: {}", sqlFunctionWrapper);
      final String substitutionVar = "${fieldSql}";
      final String template;
      final Map<String, Object> params;
      if (sqlFunctionWrapper.contains(substitutionVar)) {
        template = sqlFunctionWrapper;
        params = Map.of("fieldSql", sql);
      } else {
        template = "${functionName}(${fieldSql})";
        params = Map.of("functionName", sqlFunctionWrapper, "fieldSql", sql);
      }
      return StringSubstitutor.replace(template, params);
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
