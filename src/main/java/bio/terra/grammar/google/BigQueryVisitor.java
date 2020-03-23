package bio.terra.grammar.google;

import bio.terra.grammar.DatasetAwareVisitor;
import bio.terra.grammar.SQLParser;
import bio.terra.model.DatasetModel;

import java.util.Map;

public class BigQueryVisitor extends DatasetAwareVisitor {

    public BigQueryVisitor(Map<String, DatasetModel> datasetMap) {
        super(datasetMap);
    }

    @Override
    public String visitTable_expr(SQLParser.Table_exprContext ctx) {
        String datasetName = getNameFromContext(ctx.dataset_name());
        DatasetModel dataset = getDatasetByName(datasetName);
        String dataProjectId = dataset.getDataProject();
        String tableName = getNameFromContext(ctx.table_name());
        return String.format("`%s.%s.%s`", dataProjectId, datasetName, tableName);
    }

    @Override
    public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
        String datasetName = getNameFromContext(ctx.dataset_name());
        DatasetModel dataset = getDatasetByName(datasetName);
        String dataProjectId = dataset.getDataProject();
        String tableName = getNameFromContext(ctx.table_name());
        String columnName = getNameFromContext(ctx.column_name());
        return String.format("`%s.%s.%s.%s`", dataProjectId, datasetName, tableName, columnName);
    }
}
