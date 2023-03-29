package bio.terra.tanagra.query;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.text.StringSubstitutor;

public final class TableVariable implements SQLExpression {
  private String alias;
  private final TablePointer tablePointer;
  private final String joinField;
  private final FieldVariable joinFieldOnParent;
  private final boolean isLeftJoin;

  private TableVariable(
      TablePointer tablePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    this.tablePointer = tablePointer;
    this.joinField = joinField;
    this.joinFieldOnParent = joinFieldOnParent;
    this.isLeftJoin = isLeftJoin;
  }

  public static TableVariable forPrimary(TablePointer tablePointer) {
    return new TableVariable(tablePointer, null, null, false);
  }

  public static TableVariable forJoined(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(tablePointer, joinField, joinFieldOnParent, false);
  }

  public static TableVariable forLeftJoined(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    return forJoined(tablePointer, joinField, joinFieldOnParent, true);
  }

  private static TableVariable forJoined(
      TablePointer tablePointer,
      String joinField,
      FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    return new TableVariable(tablePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  @Override
  public String renderSQL() {
    String sql = tablePointer.renderSQL();

    String template;
    Map<String, String> params;
    if (alias != null) {
      template = "${sql} AS ${tableAlias}";
      params =
          ImmutableMap.<String, String>builder().put("sql", sql).put("tableAlias", alias).build();
      sql = StringSubstitutor.replace(template, params);
    }

    if (joinField != null) {
      template =
          "${joinType} ${tableReference} ON ${tableAlias}.${joinField} = ${joinFieldOnParent}";
      params =
          ImmutableMap.<String, String>builder()
              .put("joinType", isLeftJoin ? "LEFT JOIN" : "JOIN")
              .put("tableReference", sql)
              .put("tableAlias", alias)
              .put("joinField", joinField)
              .put("joinFieldOnParent", joinFieldOnParent.renderSQL())
              .build();
      sql = StringSubstitutor.replace(template, params);
    }
    return sql;
  }

  /**
   * Iterate through all the {@link TableVariable}s and generate a unique alias for each one. Start
   * with the default alias (= first letter of the table name) and if that's taken, append
   * successively higher integers until we find one that doesn't conflict with any other table
   * aliases.
   */
  public static void generateAliases(List<TableVariable> tableVariables) {
    Map<String, TableVariable> aliases = new HashMap<>();
    for (TableVariable tableVariable : tableVariables) {
      String defaultAlias = tableVariable.getDefaultAlias();
      String alias = defaultAlias;
      int suffix = 0;
      while (aliases.containsKey(alias)) {
        alias = defaultAlias + suffix++;
      }
      tableVariable.setAlias(alias);
      aliases.put(alias, tableVariable);
    }
  }

  /** Default table alias is the first letter of the table name. */
  private String getDefaultAlias() {
    return tablePointer.getTableName() != null
        ? tablePointer.getTableName().toLowerCase().substring(0, 1)
        : "x";
  }

  public String getAlias() {
    return alias;
  }

  private void setAlias(String alias) {
    this.alias = alias;
  }

  public TablePointer getTablePointer() {
    return tablePointer;
  }

  public boolean isPrimary() {
    return joinField == null;
  }
}