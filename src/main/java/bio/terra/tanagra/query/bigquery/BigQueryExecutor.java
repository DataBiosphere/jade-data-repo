package bio.terra.tanagra.query.bigquery;

import bio.terra.service.dataset.DatasetTable;
import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.SqlPlatform;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.datapointer.BigQueryDataset;
import bio.terra.tanagra.utils.GoogleBigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link QueryExecutor} for Google BigQuery. */
public class BigQueryExecutor implements QueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryExecutor.class);

  /** The BigQuery client to use for executing queries. */
  private final GoogleBigQuery bigQuery;

  public BigQueryExecutor(GoogleBigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  @Override
  public QueryResult execute(QueryRequest queryRequest) {
    String sql = renderSQL(queryRequest.query());
    LOGGER.info("Running SQL against BigQuery: {}", sql);
    TableResult tableResult = bigQuery.queryBigQuery(sql);

    Iterable<RowResult> rowResults =
        Iterables.transform(
            tableResult.getValues(),
            (FieldValueList fieldValueList) ->
                new BigQueryRowResult(fieldValueList, queryRequest.columnHeaderSchema()));

    return new QueryResult(rowResults, queryRequest.columnHeaderSchema());
  }

  @Override
  public void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun) {
    throw new NotImplementedException();
  }

  @Override
  public Collection<RowResult> readTableRows(Query query) {
    throw new NotImplementedException();
  }

  @Override
  public SqlPlatform getSqlPlatform() {
    return SqlPlatform.BIGQUERY;
  }

  @Override
  public DatasetTable getSchema(UUID datasetId, String tableName) {
    throw new NotImplementedException();
  }

  private BigQueryDataset getBQDataPointer(TablePointer tablePointer) {
    DataPointer outputDataPointer = tablePointer.getDataPointer();
    if (outputDataPointer instanceof BigQueryDataset bigQueryDataset) {
      return bigQueryDataset;
    }
    throw new InvalidConfigException("Entity indexing job only supports BigQuery");
  }

  @Override
  public void deleteTable(TablePointer tablePointer, boolean isDryRun) {
    if (!checkTableExists(tablePointer) || isDryRun) {
      return;
    }
    LOGGER.info("Delete table: {}", tablePointer.getPathForIndexing());
    BigQueryDataset outputBQDataset = getBQDataPointer(tablePointer);
    outputBQDataset
        .getBigQueryService()
        .deleteTable(
            outputBQDataset.getProjectId(),
            outputBQDataset.getDatasetId(),
            tablePointer.getTableName());
  }

  @Override
  public boolean checkTableExists(TablePointer tablePointer) {
    BigQueryDataset outputBQDataset = getBQDataPointer(tablePointer);
    LOGGER.info(
        "output BQ table: project={}, dataset={}, table={}",
        outputBQDataset.getProjectId(),
        outputBQDataset.getDatasetId(),
        tablePointer.getTableName());
    GoogleBigQuery googleBigQuery = outputBQDataset.getBigQueryService();
    // FIXME: use indexExecutor
    return googleBigQuery
        .getTable(
            outputBQDataset.getProjectId(),
            outputBQDataset.getDatasetId(),
            tablePointer.getTableName())
        .isPresent();
  }
}
