package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public record TablePointer(String tableName, Filter filter) implements SqlExpression {

  public static TablePointer fromTableName(String tableName) {
    return new TablePointer(tableName, null);
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    if (filter == null) {
      return context.getTableName(tableName);
    }

    TablePointer tablePointerWithoutFilter = TablePointer.fromTableName(tableName);
    TableVariable tableVar = TableVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query =
        new Query.Builder()
            .select(List.of(fieldVar))
            .addTables(List.of(tableVar))
            .addWhere(filterVar)
            .build();
    return "(" + query.renderSQL(context) + ")";
  }
}
