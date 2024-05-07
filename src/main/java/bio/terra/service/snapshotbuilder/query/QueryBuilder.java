package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public class QueryBuilder {
  private List<SelectExpression> select;
  private List<TableVariable> tables;
  private FilterVariable where;
  private List<FieldVariable> groupBy;
  private List<OrderByVariable> orderBy;
  private Integer limit;

  public QueryBuilder() {}

  public QueryBuilder addSelect(List<SelectExpression> select) {
    this.select = select;
    return this;
  }

  public QueryBuilder addTables(List<TableVariable> tables) {
    this.tables = tables;
    return this;
  }

  public QueryBuilder addWhere(FilterVariable where) {
    this.where = where;
    return this;
  }

  public QueryBuilder addGroupBy(List<FieldVariable> groupBy) {
    this.groupBy = groupBy;
    return this;
  }

  public QueryBuilder addOrderBy(List<OrderByVariable> orderBy) {
    this.orderBy = orderBy;
    return this;
  }

  public QueryBuilder addLimit(Integer limit) {
    this.limit = limit;
    return this;
  }

  public Query build() {
    return new Query(this.select, this.tables, this.where, this.groupBy, this.orderBy, this.limit);
  }
}
