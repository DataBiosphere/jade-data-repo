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
    DatasetModel dataset = getDatasetByName(datasetName);
    String tableName = getNameFromContext(ctx.table_name());
    String bqTableName = generateTableName(dataset, tableName);
    String alias = generateAlias(bqTableName, tableName);
    return String.format("%s AS `%s`", bqTableName, alias);
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
    return (tableName) -> generateTableName(dataset, tableName);
  }

  private static String generateTableName(DatasetModel dataset, String tableName) {
    String dataProjectId = dataset.getDataProject();
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + dataset.getName();
    return String.format("`%s.%s.%s`", dataProjectId, bqDatasetName, tableName);
  }
}
