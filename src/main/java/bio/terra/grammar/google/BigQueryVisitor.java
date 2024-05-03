package bio.terra.grammar.google;

import bio.terra.common.PdaoConstant;
import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import java.util.Map;
import java.util.Objects;

public class BigQueryVisitor extends DatasetAwareVisitor {

  public BigQueryVisitor(Map<String, DatasetModel> datasetMap) {
    super(datasetMap);
  }

  public String generateAlias(String datasetName, String tableName) {
    return "alias" + Math.abs(Objects.hash(datasetName, tableName));
  }

  @Override
  public String visitTable_expr(SQLParser.Table_exprContext ctx) {
    String datasetName = getNameFromContext(ctx.dataset_name());
    String bqDatasetName = prefixDatasetName(datasetName);
    DatasetModel dataset = getDatasetByName(datasetName);
    String tableName = getNameFromContext(ctx.table_name());
    return String.format(
        "%s AS `%s`",
        generateTableName(dataset, datasetName, tableName),
        generateAlias(bqDatasetName, tableName));
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + getNameFromContext(ctx.dataset_name());
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(bqDatasetName, tableName);
    String columnName = getNameFromContext(ctx.column_name());
    return String.format("`%s`.%s", alias, columnName);
  }

  public static TableNameGenerator bqTableName(DatasetModel dataset) {
    return (tableName) -> generateTableName(dataset, dataset.getName(), tableName);
  }

  private static String generateTableName(
      DatasetModel dataset, String datasetName, String tableName) {
    return String.format(
        "`%s.%s.%s`", dataset.getDataProject(), prefixDatasetName(datasetName), tableName);
  }

  private static String prefixDatasetName(String datasetName) {
    return PdaoConstant.PDAO_PREFIX + datasetName;
  }
}
