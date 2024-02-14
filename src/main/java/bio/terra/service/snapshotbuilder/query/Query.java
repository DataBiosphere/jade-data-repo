package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.filtervariable.HavingFilterVariable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public record Query(
    List<FieldVariable> select,
    List<TableVariable> tables,
    FilterVariable where,
    List<FieldVariable> groupBy,
    HavingFilterVariable having,
    Integer limit)
    implements SqlExpression {

  public Query {
    if (select.isEmpty()) {
      throw new IllegalArgumentException("Query must have at least one SELECT FieldVariable");
    }
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("Query must have at least one TableVariable");
    }
    if (groupBy == null) {
      groupBy = List.of();
    }
    long primaryTables = tables.stream().filter(TableVariable::isPrimary).count();
    if (primaryTables != 1) {
      throw new IllegalArgumentException(
          "Query can only have one primary table, but found " + primaryTables);
    }
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables) {
    this(select, tables, null, null, null, null);
  }

  public Query(
      List<FieldVariable> select, List<TableVariable> tables, List<FieldVariable> groupBy) {
    this(select, tables, null, groupBy, null, null);
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables, FilterVariable where) {
    this(select, tables, where, null, null, null);
  }

  public Query(
      List<FieldVariable> select, List<TableVariable> tables, FilterVariable where, Integer limit) {
    this(select, tables, where, null, null, limit);
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    // generate a unique alias for each TableVariable
    TableVariable.generateAliases(tables);

    // render each SELECT FieldVariable and join them into a single string
    String selectSQL =
        select.stream()
            .sorted(Comparator.comparing(FieldVariable::getAlias))
            .map(fieldVar -> fieldVar.renderSQL(platform))
            .collect(Collectors.joining(", "));

    // render the primary TableVariable
    String sql =
        new ST("SELECT <selectSQL> FROM <primaryTableFromSQL>")
            .add("selectSQL", selectSQL)
            .add("primaryTableFromSQL", getPrimaryTable().renderSQL(platform))
            .render();

    // render the join TableVariables
    if (tables.size() > 1) {
      sql =
          new ST("<sql> <joinTablesFromSQL>")
              .add("sql", sql)
              .add(
                  "joinTablesFromSQL",
                  tables.stream()
                      .map(tv -> tv.isPrimary() ? "" : tv.renderSQL(platform))
                      .collect(Collectors.joining(" ")))
              .render();
    }

    // render the FilterVariable
    if (where != null) {
      sql =
          new ST("<sql> WHERE <whereSQL>")
              .add("sql", sql)
              .add("whereSQL", where.renderSQL(platform))
              .render();
    }

    if (!groupBy.isEmpty()) {
      // render each GROUP BY FieldVariable and join them into a single string
      sql =
          new ST("<sql> GROUP BY <groupBySQL>")
              .add("sql", sql)
              .add(
                  "groupBySQL",
                  groupBy.stream()
                      .map(fv -> fv.renderSqlForOrderOrGroupBy(select.contains(fv)))
                      .collect(Collectors.joining(", ")))
              .render();
    }

    if (having != null) {
      sql += " " + having.renderSQL(platform);
    }

    // TODO: DC-836 Implement LIMIT for Azure (TOP N + sql)
    //  This means passing in the platform to renderSQL
    //  and refactoring the current TableVariable and tableNameGenerator work
    if (limit != null) {
      sql += " LIMIT " + limit;
    }

    return sql;
  }

  public List<FieldVariable> getSelect() {
    return List.copyOf(select);
  }

  public TableVariable getPrimaryTable() {
    return tables.stream().filter(TableVariable::isPrimary).findFirst().orElseThrow();
  }
}
