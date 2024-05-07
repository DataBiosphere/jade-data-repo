package bio.terra.service.snapshotbuilder.query;

import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public record Query(
    List<SelectExpression> select,
    List<TableVariable> tables,
    FilterVariable where,
    List<FieldVariable> groupBy,
    List<OrderByVariable> orderBy,
    Integer limit)
    implements SqlExpression {

  public Query {
    if (select.isEmpty()) {
      throw new IllegalArgumentException("Query must have at least one SELECT FieldVariable");
    }
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("Query must have at least one TableVariable");
    }
    groupBy = groupBy != null ? groupBy : List.of();
    orderBy = orderBy != null ? orderBy : List.of();

    long primaryTables = tables.stream().filter(TableVariable::isPrimary).count();
    if (primaryTables != 1) {
      throw new IllegalArgumentException(
          "Query can only have one primary table, but found " + primaryTables);
    }
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    // render each SELECT FieldVariable and join them into a single string
    String selectSQL =
        select.stream()
            .map(fieldVar -> fieldVar.renderSQL(context))
            .collect(Collectors.joining(", "));

    // render the primary TableVariable
    String sql =
        new ST("<selectSQL> FROM <primaryTableFromSQL>")
            .add("selectSQL", selectSQL)
            .add("primaryTableFromSQL", getPrimaryTable().renderSQL(context))
            .render();

    // render the join TableVariables
    if (tables.size() > 1) {
      sql =
          new ST("<sql> <joinTablesFromSQL>")
              .add("sql", sql)
              .add(
                  "joinTablesFromSQL",
                  tables.stream()
                      .map(tv -> tv.isPrimary() ? "" : tv.renderSQL(context))
                      .collect(Collectors.joining(" ")))
              .render();
    }

    // render the FilterVariable
    if (where != null) {
      sql =
          new ST("<sql> WHERE <whereSQL>")
              .add("sql", sql)
              .add("whereSQL", where.renderSQL(context))
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
                      .map(fv -> fv.renderSqlForOrderOrGroupBy(select.contains(fv), context))
                      .collect(Collectors.joining(", ")))
              .render();
    }

    if (!orderBy.isEmpty()) {
      sql =
          new ST("<sql> ORDER BY <orderBySQL>")
              .add("sql", sql)
              .add(
                  "orderBySQL",
                  orderBy.stream()
                      .map(fv -> fv.renderSQL(true, context))
                      .collect(Collectors.joining(", ")))
              .render();
    }

    if (limit != null) {
      var prevSql = sql;
      sql =
          context.getPlatform().choose(prevSql + " LIMIT " + limit, "TOP " + limit + " " + prevSql);
    }

    return "SELECT " + sql;
  }

  public TableVariable getPrimaryTable() {
    return tables.stream().filter(TableVariable::isPrimary).findFirst().orElseThrow();
  }
}
