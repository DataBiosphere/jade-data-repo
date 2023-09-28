package bio.terra.tanagra.query;

import bio.terra.tanagra.underlay.DataPointer;
import java.util.List;

public record TablePointer(DataPointer dataPointer, String tableName, Filter filter, String sql) implements SQLExpression {

  public static TablePointer fromTableName(String tableName, DataPointer dataPointer) {
    return new Builder().dataPointer(dataPointer).tableName(tableName).build();
  }

  public static TablePointer fromRawSql(String sql, DataPointer dataPointer) {
    return new Builder().dataPointer(dataPointer).sql(sql).build();
  }
  public boolean isRawSql() {
    return sql != null;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    if (sql != null) {
      return "(" + sql + ")";
    }
    if (filter == null) {
      return dataPointer.getTableSQL(tableName);
    }

    TablePointer tablePointerWithoutFilter = TablePointer.fromTableName(tableName, dataPointer);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query =
        new Query.Builder()
            .select(List.of(fieldVar))
            .tables(List.of(tableVar))
            .where(filterVar)
            .build();
    return "(" + query.renderSQL(platform) + ")";
  }

  public String getPathForIndexing() {
    return dataPointer.getTablePathForIndexing(tableName);
  }

  public FilterVariable getFilterVariable(TableVariable tableVariable, List<TableVariable> tables) {
    if (filter == null) {
      return null;
    }
    return filter.buildVariable(tableVariable, tables);
  }

  public static class Builder {
    private DataPointer dataPointer;
    private String tableName;
    private Filter filter;
    private String sql;

    public Builder dataPointer(DataPointer dataPointer) {
      this.dataPointer = dataPointer;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder tableFilter(Filter filter) {
      this.filter = filter;
      return this;
    }

    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    /** Call the private constructor. */
    public TablePointer build() {
      return new TablePointer(dataPointer, tableName, filter, sql);
    }
  }
}
