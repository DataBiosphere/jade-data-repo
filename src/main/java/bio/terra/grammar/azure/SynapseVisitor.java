package bio.terra.grammar.azure;

import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;
import java.util.Map;

public class SynapseVisitor extends DatasetAwareVisitor {

  private static final int PRIME = 31;
  private final String sourceDatasetDatasource;

  public SynapseVisitor(Map<String, DatasetModel> datasetMap, String sourceDatasetDatasource) {
    super(datasetMap);
    this.sourceDatasetDatasource = sourceDatasetDatasource;
  }

  public String generateAlias(String tableName) {
    int tableNameHash = tableName.hashCode();
    // there's less of a chance of collision if we multiply the first value by an odd prime before
    // we sum them
    int hash = Math.abs(tableNameHash * PRIME);
    return "alias" + hash;
  }

  @Override
  public String visitTable_expr(SQLParser.Table_exprContext ctx) {
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(tableName);
    return """
      OPENROWSET(
        BULK 'parquet/%s/*/*.parquet',
        DATA_SOURCE = '%s',
        FORMAT = 'parquet') AS %s
      """
        .formatted(tableName, sourceDatasetDatasource, alias);
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(tableName);
    String columnName = getNameFromContext(ctx.column_name());
    return String.format("%s.%s", alias, columnName);
  }
}
