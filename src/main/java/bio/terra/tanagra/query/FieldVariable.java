package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import bio.terra.tanagra.exception.SystemException;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldVariable implements SQLExpression {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldVariable.class);
  private final FieldPointer fieldPointer;
  private final TableVariable tableVariable;
  private String alias;

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable) {
    this.fieldPointer = fieldPointer;
    this.tableVariable = tableVariable;
  }

  public FieldVariable(FieldPointer fieldPointer, TableVariable tableVariable, String alias) {
    this.fieldPointer = fieldPointer;
    this.tableVariable = tableVariable;
    this.alias = alias;
  }

  @Override
  public String renderSQL(CloudPlatform platform) {
    return renderSQL(true, true);
  }

  public String renderSqlForOrderBy() {
    return renderSQL(false, false);
  }

  public String renderSqlForWhere() {
    return renderSQL(false, true);
  }

  private String renderSQL(boolean useAlias, boolean useFunctionWrapper) {
    String template = "${tableAlias}.${columnName}";
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("tableAlias", tableVariable.getAlias())
            .put("columnName", fieldPointer.getColumnName())
            .build();
    String sql = StringSubstitutor.replace(template, params);

    if (fieldPointer.isForeignKey()) {
      throw new SystemException("TODO: implement embedded selects " + sql);
    }

    if (fieldPointer.hasSqlFunctionWrapper() && useFunctionWrapper) {
      LOGGER.debug("Found sql function wrapper: " + fieldPointer.getSqlFunctionWrapper());
      final String substitutionVar = "${fieldSql}";
      if (fieldPointer.getSqlFunctionWrapper().contains(substitutionVar)) {
        template = fieldPointer.getSqlFunctionWrapper();
        params = ImmutableMap.<String, String>builder().put("fieldSql", sql).build();
      } else {
        template = "${functionName}(${fieldSql})";
        params =
            ImmutableMap.<String, String>builder()
                .put("functionName", fieldPointer.getSqlFunctionWrapper())
                .put("fieldSql", sql)
                .build();
      }
      sql = StringSubstitutor.replace(template, params);
    }

    if (alias != null && useAlias) {
      template = "${fieldSql} AS ${fieldAlias}";
      params =
          ImmutableMap.<String, String>builder()
              .put("fieldSql", sql)
              .put("fieldAlias", alias)
              .build();
      sql = StringSubstitutor.replace(template, params);
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
    if (!fieldPointer.isForeignKey()) {
      return fieldPointer.getColumnName();
    } else {
      return fieldPointer.getForeignColumnName();
    }
  }

  public FieldPointer getFieldPointer() {
    return fieldPointer;
  }
}
