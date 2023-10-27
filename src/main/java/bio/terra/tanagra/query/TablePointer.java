package bio.terra.tanagra.query;

import java.util.List;

public record TablePointer(String tableName, Filter filter, String sql) implements SQLExpression {

  public static TablePointer fromTableName(String tableName) {
    return new TablePointer(tableName, null, null);
  }

  public static TablePointer fromRawSql(String sql) {
    return new TablePointer(null, null, sql);
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    if (sql != null) {
      return "(" + sql + ")";
    }
    if (filter == null) {
      // TODO: use platform to render tableName correctly (or fix it using DatasetAwareVisitor).
      return tableName;
    }

    TablePointer tablePointerWithoutFilter = TablePointer.fromTableName(tableName);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query = new Query(List.of(fieldVar), List.of(tableVar), filterVar);
    return "(" + query.renderSQL(platform) + ")";
  }
}
