package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.filtervariable.HavingFilterVariable;
import java.util.List;

public class QueryBuilder {
  private List<SelectExpression> select = null;
  private List<TableVariable> tables = null;
  private FilterVariable where = null;
  private List<FieldVariable> groupBy = null;
  private HavingFilterVariable having = null;
  private List<OrderByVariable> orderBy = null;
  private Integer limit = null;

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

  public QueryBuilder addHaving(HavingFilterVariable having) {
    this.having = having;
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
    return new Query(
        this.select, this.tables, this.where, this.groupBy, this.having, this.orderBy, this.limit);
  }
}
