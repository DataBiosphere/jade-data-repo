package bio.terra.datarepo.grammar.google;

import bio.terra.datarepo.common.PdaoConstant;
import bio.terra.datarepo.grammar.DatasetAwareVisitor;
import bio.terra.datarepo.grammar.SQLParser;
import bio.terra.datarepo.model.DatasetModel;
import java.util.Map;

public class BigQueryVisitor extends DatasetAwareVisitor {

  private static final int PRIME = 31;

  public BigQueryVisitor(Map<String, DatasetModel> datasetMap) {
    super(datasetMap);
  }

  public String generateAlias(String datasetName, String tableName) {
    int datasetNameHash = datasetName.hashCode();
    int tableNameHash = tableName.hashCode();
    // there's less of a chance of collision if we multiply the first value by an odd prime before
    // we sum them
    int hash = datasetNameHash * PRIME + tableNameHash;
    return "alias" + String.valueOf(hash);
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
