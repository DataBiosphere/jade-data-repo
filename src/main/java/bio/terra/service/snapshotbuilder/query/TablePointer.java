package bio.terra.service.snapshotbuilder.query;

import java.util.List;
import java.util.function.Function;

public record TablePointer(
    String tableName, Filter filter, String sql, Function<String, String> generateTableName)
    implements SqlExpression {

  public static TablePointer fromTableName(String tableName) {
    return new TablePointer(tableName, null, null, Function.identity());
  }

  public static TablePointer fromTableName(
      String tableName, Function<String, String> generateTableName) {
    return new TablePointer(tableName, null, null, generateTableName);
  }

  public static TablePointer fromRawSql(String sql) {
    return new TablePointer(null, null, sql, Function.identity());
  }

  @Override
  public String renderSQL() {
    if (sql != null) {
      return "(" + sql + ")";
    }
    if (filter == null) {
      return generateTableName.apply(tableName);
    }

    TablePointer tablePointerWithoutFilter =
        TablePointer.fromTableName(tableName, generateTableName);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query = new Query(List.of(fieldVar), List.of(tableVar), filterVar);
    return "(" + query.renderSQL() + ")";
  }
}
