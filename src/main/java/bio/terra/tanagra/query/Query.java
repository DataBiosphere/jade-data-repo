package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.filtervariable.HavingFilterVariable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public record Query(
    List<FieldVariable> select,
    List<TableVariable> tables,
    FilterVariable where,
    List<OrderByVariable> orderBy,
    List<FieldVariable> groupBy,
    HavingFilterVariable having,
    Integer limit)
    implements SQLExpression {

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
    if (orderBy == null) {
      orderBy = List.of();
    }
    if (!groupBy.isEmpty() && !orderBy.isEmpty()) {
      throw new IllegalArgumentException("Query cannot have both GROUP BY and ORDER BY");
    }
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables) {
    this(select, tables, null, null, null, null, null);
  }

  public Query(
      List<FieldVariable> select, List<TableVariable> tables, List<FieldVariable> groupBy) {
    this(select, tables, null, null, groupBy, null, null);
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables, FilterVariable where) {
    this(select, tables, where, null, null, null, null);
  }

  public Query(
      List<FieldVariable> select,
      List<TableVariable> tables,
      List<OrderByVariable> orderBy,
      Integer limit) {
    this(select, tables, null, orderBy, null, null, limit);
  }

  public Query(
      List<FieldVariable> select,
      List<TableVariable> tables,
      List<OrderByVariable> orderBy,
      List<FieldVariable> groupBy,
      Integer limit) {
    this(select, tables, null, orderBy, groupBy, null, limit);
  }

  public Query(List<FieldVariable> select, List<TableVariable> tables, int limit) {
    this(select, tables, null, null, null, null, limit);
  }

  public static final String TANAGRA_FIELD_PREFIX = "t_";

  @Override
  public String renderSQL(SqlPlatform platform) {
    // generate a unique alias for each TableVariable
    TableVariable.generateAliases(tables);

    // render each SELECT FieldVariable and join them into a single string
    String selectSQL =
        select.stream()
            .sorted(Comparator.comparing(FieldVariable::getAlias))
            .map(fieldVariable -> fieldVariable.renderSQL(platform))
            .collect(Collectors.joining(", "));

    if (platform == SqlPlatform.SYNAPSE && limit != null) {
      selectSQL = "TOP " + limit + " " + selectSQL;
    }

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

    if (platform == SqlPlatform.BIGQUERY && !orderBy.isEmpty()) {
      // render each ORDER BY FieldVariable and join them into a single string
      sql =
          new ST("<sql> ORDER BY <orderBySQL>")
              .add("sql", sql)
              .add(
                  "orderBySQL",
                  orderBy.stream()
                      .map(obv -> obv.renderSQL(platform, select.contains(obv.fieldVariable)))
                      .collect(Collectors.joining(", ")))
              .render();
    }

    if (platform == SqlPlatform.BIGQUERY && limit != null) {
      sql = new ST("<sql> LIMIT <limit>").add("sql", sql).add("limit", limit).render();
    }

    return sql;
  }

  public List<FieldVariable> getSelect() {
    return Collections.unmodifiableList(select);
  }

  public TableVariable getPrimaryTable() {
    List<TableVariable> primaryTable = tables.stream().filter(TableVariable::isPrimary).toList();
    if (primaryTable.size() != 1) {
      throw new SystemException(
          "Query can only have one primary table, but found " + primaryTable.size());
    }
    return primaryTable.get(0);
  }
}
