package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import java.util.List;

public record TablePointer(
    String tableName, Filter filter, String sql, TableNameGenerator generateTableName)
    implements SqlExpression {

  public static TablePointer fromTableName(String tableName, TableNameGenerator generateTableName) {
    return new TablePointer(tableName, null, null, generateTableName);
  }

  public static TablePointer fromRawSql(String sql) {
    return new TablePointer(null, null, sql, s -> s);
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    if (sql != null) {
      return "(" + sql + ")";
    }
    if (filter == null) {
      return generateTableName.generate(tableName);
    }

    TablePointer tablePointerWithoutFilter =
        TablePointer.fromTableName(tableName, generateTableName);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query = new Query(List.of(fieldVar), List.of(tableVar), filterVar);
    return "(" + query.renderSQL(platform) + ")";
  }
}
