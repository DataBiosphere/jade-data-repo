package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.table.Table;
import java.util.List;

public record TablePointer(String tableName, Filter filter) implements SourcePointer {

  public static TablePointer fromTableName(String tableName) {
    return new TablePointer(tableName, null);
  }

  @Override
  public String getSourceName() {
    return tableName;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    if (filter == null) {
      return context.getTableName(tableName);
    }

    TablePointer tablePointerWithoutFilter = TablePointer.fromTableName(tableName);
    SourceVariable tableVar = SourceVariable.forPrimary(tablePointerWithoutFilter);
    FieldVariable fieldVar =
        new FieldVariable(FieldPointer.allFields(tablePointerWithoutFilter), tableVar);
    FilterVariable filterVar = filter.buildVariable(tableVar, List.of(tableVar));

    Query query =
        new Query.Builder()
            .select(List.of(fieldVar))
            .tables(List.of(new Table(tableVar)))
            .where(filterVar)
            .build();
    return "(" + query.renderSQL(context) + ")";
  }
}
