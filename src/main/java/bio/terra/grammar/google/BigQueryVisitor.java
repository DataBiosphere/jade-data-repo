package bio.terra.grammar.google;

import bio.terra.common.PdaoConstant;
import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotModel;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Objects;

public class BigQueryVisitor extends DatasetAwareVisitor {

  public BigQueryVisitor(Map<String, DatasetModel> datasetMap) {
    super(datasetMap);
  }

  @VisibleForTesting
  public static String generateAlias(String datasetName, String tableName) {
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
        generateTableName(dataset.getDataProject(), bqDatasetName, tableName),
        generateAlias(bqDatasetName, tableName));
  }

  private static String generateTableName(String dataProject, String name, String tableName) {
    return String.format("`%s.%s.%s`", dataProject, name, tableName);
  }

  @Override
  public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
    String datasetName = getNameFromContext(ctx.dataset_name());
    String tableName = getNameFromContext(ctx.table_name());
    if (tableName == null) {
      throw new InvalidQueryException(
          "All column names must be qualified with a dataset and table name. "
              + "Please ensure that your query uses the format `dataset.table.column` for columns. "
              + "For example, use `my_dataset.my_table.my_column` instead of just `my_column`."
              + "Unqualified column name: "
              + getNameFromContext(ctx.column_name()));
    }
    String alias = generateAlias(prefixDatasetName(datasetName), tableName);
    String columnName = getNameFromContext(ctx.column_name());
    return String.format("`%s`.%s", alias, columnName);
  }

  public static TableNameGenerator bqDatasetTableName(DatasetModel dataset) {
    return tableName ->
        generateTableName(
            dataset.getDataProject(), prefixDatasetName(dataset.getName()), tableName);
  }

  public static TableNameGenerator bqSnapshotTableName(SnapshotModel snapshot) {
    return tableName -> generateTableName(snapshot.getDataProject(), snapshot.getName(), tableName);
  }

  private static String prefixDatasetName(String datasetName) {
    return PdaoConstant.PDAO_PREFIX + datasetName;
  }
}
