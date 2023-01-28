package bio.terra.grammar.google;

import bio.terra.common.PdaoConstant;
import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;
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
    String dataProjectId = dataset.getDataProject();
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + getNameFromContext(ctx.dataset_name());
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(bqDatasetName, tableName);
    return String.format("`%s.%s.%s` AS `%s`", dataProjectId, bqDatasetName, tableName, alias);
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + getNameFromContext(ctx.dataset_name());
    String tableName = getNameFromContext(ctx.table_name());
    String alias = generateAlias(bqDatasetName, tableName);
    String columnName = getNameFromContext(ctx.column_name());
    return String.format("`%s`.%s", alias, columnName);
  }
}
