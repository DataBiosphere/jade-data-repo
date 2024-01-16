package bio.terra.grammar.azure;

import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import java.util.Map;
import java.util.Objects;

public class SynapseVisitor extends DatasetAwareVisitor {
  private final String sourceDatasetDatasource;

  public SynapseVisitor(Map<String, DatasetModel> datasetMap, String sourceDatasetDatasource) {
    super(datasetMap);
    this.sourceDatasetDatasource = sourceDatasetDatasource;
  }

  public static String generateAlias(String tableName) {
    return "alias" + Math.abs(Objects.hash(tableName));
  }

  @Override
  public String visitTable_expr(SQLParser.Table_exprContext ctx) {
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(tableName);
    return """
      (SELECT * FROM
      OPENROWSET(
        BULK '%s',
        DATA_SOURCE = '%s',
        FORMAT = 'parquet') AS %s) AS %s
      """
        .formatted(
            FolderType.METADATA.getPath("parquet/%s/*/*.parquet".formatted(tableName)),
            sourceDatasetDatasource,
            "inner_" + alias,
            alias);
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String columnName = getNameFromContext(ctx.column_name());
    // if column_name is qualified by both dataset & table, then tableName lives at ctx.table_name()
    // e.g. dataset.table.column
    String tableName = getNameFromContext(ctx.table_name());
    if (tableName == null) {
      // if column_name is only qualified by the table, then tableName lives at ctx.alias_name()
      // e.g. table.column
      tableName = getNameFromContext(ctx.alias_name());
      // if both ctx.table_name() and ctx.alias_name() are null, then let's just return the column
      if (tableName == null) {
        return columnName;
      }
    }
    // We expect this alias to match the one generated in visitTable_expr
    String alias = generateAlias(tableName);
    return String.format("%s.%s", alias, columnName);
  }

  @Override
  public String visitQuoted_string(SQLParser.Quoted_stringContext ctx) {
    String quotedString = ctx.getText();
    // Quoted values in synapse must be single quoted
    return quotedString.replace("\"", "'");
  }

  public static TableNameGenerator azureTableName(String sourceDatasetDatasource) {
    return (tableName) -> {
      String alias = generateAlias(tableName);
      return "(SELECT * FROM OPENROWSET(BULK '%s', DATA_SOURCE = '%s', FORMAT = 'parquet') AS %s)"
          .formatted(
              FolderType.METADATA.getPath(
                  "parquet/%s/*/*.parquet".formatted(tableName)),
              sourceDatasetDatasource,
              alias);
    };
  }
}
