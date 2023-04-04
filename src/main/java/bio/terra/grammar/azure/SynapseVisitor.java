package bio.terra.grammar.azure;

import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import java.util.Map;
import java.util.Objects;

public class SynapseVisitor extends DatasetAwareVisitor {
  private final String sourceDatasetDatasource;

  public SynapseVisitor(Map<String, DatasetModel> datasetMap, String sourceDatasetDatasource) {
    super(datasetMap);
    this.sourceDatasetDatasource = sourceDatasetDatasource;
  }

  public String generateAlias(String tableName) {
    return "alias" + Math.abs(Objects.hash(tableName));
  }

  @Override
  public String visitTable_expr(SQLParser.Table_exprContext ctx) {
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(tableName);
    return """
      OPENROWSET(
        BULK '%s',
        DATA_SOURCE = '%s',
        FORMAT = 'parquet') AS %s
      """
        .formatted(
            FolderType.METADATA.getPath("parquet/%s/*/*.parquet".formatted(tableName)),
            sourceDatasetDatasource,
            alias);
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(tableName);
    String columnName = getNameFromContext(ctx.column_name());
    return String.format("%s.%s", alias, columnName);
  }
}
