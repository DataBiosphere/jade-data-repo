package bio.terra.tanagra.query;

import bio.terra.tanagra.query.datapointer.DataPointer;
import java.util.List;

public record TablePointer(DataPointer dataPointer, String tableName, Filter filter, String sql)
    implements SQLExpression {

  public static TablePointer fromTableName(DataPointer dataPointer, String tableName) {
    return new TablePointer(dataPointer, tableName, null, null);
  }

  public static TablePointer fromRawSql(DataPointer dataPointer, String sql) {
    return new TablePointer(dataPointer, null, null, sql);
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

    TablePointer tablePointerWithoutFilter = TablePointer.fromTableName(dataPointer, tableName);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query = new Query(List.of(fieldVar), List.of(tableVar), filterVar);
    return "(" + query.renderSQL(platform) + ")";
  }

  public String getPathForIndexing() {
    return dataPointer.getTablePathForIndexing(tableName);
  }
}
