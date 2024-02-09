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
    Integer limit,
    CloudPlatformWrapper platform)
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
    this(select, tables, null, null, null, null, null);
  }

  public Query(
      List<FieldVariable> select, List<TableVariable> tables, List<FieldVariable> groupBy) {
    this(select, tables, null, groupBy, null, null, null);
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables, FilterVariable where) {
    this(select, tables, where, null, null, null, null);
  }

  // where limit is added, platform must be added as well
  public Query(
      List<FieldVariable> select,
      List<TableVariable> tables,
      FilterVariable where,
      Integer limit,
      CloudPlatformWrapper platform) {
    this(select, tables, where, null, null, limit, platform);
  }

  @Override
  public String renderSQL() {
    // generate a unique alias for each TableVariable
    TableVariable.generateAliases(tables);

    // render each SELECT FieldVariable and join them into a single string
    String selectSQL =
        select.stream()
            .sorted(Comparator.comparing(FieldVariable::getAlias))
            .map(FieldVariable::renderSQL)
            .collect(Collectors.joining(", "));

    // render the primary TableVariable
    String sql =
        new ST("SELECT <selectSQL> FROM <primaryTableFromSQL>")
            .add("selectSQL", selectSQL)
            .add("primaryTableFromSQL", getPrimaryTable().renderSQL())
            .render();

    // render the join TableVariables
    if (tables.size() > 1) {
      sql =
          new ST("<sql> <joinTablesFromSQL>")
              .add("sql", sql)
              .add(
                  "joinTablesFromSQL",
                  tables.stream()
                      .map(tv -> tv.isPrimary() ? "" : tv.renderSQL())
                      .collect(Collectors.joining(" ")))
              .render();
    }

    // render the FilterVariable
    if (where != null) {
      sql =
          new ST("<sql> WHERE <whereSQL>")
              .add("sql", sql)
              .add("whereSQL", where.renderSQL())
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
      sql += " " + having.renderSQL();
    }

    if (limit != null) {
      if (platform != null) {
        if (platform.isGcp()) {
          sql += " LIMIT " + limit;
        } else if (platform().isAzure()) {
          sql = "TOP " + limit + " " + sql;
        }
        // if platform is not provided limit for GCP is generated
      } else {
        sql += " LIMIT " + limit;
      }
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
