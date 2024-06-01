package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.table.Table;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public record Query(
    List<SelectExpression> select,
    List<Table> tables,
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
    groupBy = Objects.requireNonNullElse(groupBy, List.of());
    orderBy = Objects.requireNonNullElse(orderBy, List.of());

    long primaryTables = tables.stream().filter(Table::isPrimary).count();
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

  public Table getPrimaryTable() {
    return tables.stream().filter(Table::isPrimary).findFirst().orElseThrow();
  }

  public static class Builder {
    private List<SelectExpression> select;
    private List<Table> tables;
    private FilterVariable where;
    private List<FieldVariable> groupBy;
    private List<OrderByVariable> orderBy;
    private Integer limit;

    public Builder() {
      // Constructor intentionally left empty to allow for custom initialization via setter methods
    }

    public Builder select(List<SelectExpression> select) {
      this.select = select;
      return this;
    }

    public Builder tables(List<Table> tables) {
      this.tables = tables;
      return this;
    }

    public Builder where(FilterVariable where) {
      this.where = where;
      return this;
    }

    public Builder groupBy(List<FieldVariable> groupBy) {
      this.groupBy = groupBy;
      return this;
    }

    public Builder orderBy(List<OrderByVariable> orderBy) {
      this.orderBy = orderBy;
      return this;
    }

    public Builder limit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public Query build() {
      return new Query(select, tables, where, groupBy, orderBy, limit);
    }
  }
}
