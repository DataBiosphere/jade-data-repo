package bio.terra.tanagra.query.bigquery;

import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.utils.GoogleBigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Iterables;
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
    String sql = queryRequest.getSql();
    LOGGER.info("Running SQL against BigQuery: {}", sql);
    TableResult tableResult = bigQuery.queryBigQuery(sql);

    Iterable<RowResult> rowResults =
        Iterables.transform(
            tableResult.getValues(),
            (FieldValueList fieldValueList) ->
                new BigQueryRowResult(fieldValueList, queryRequest.getColumnHeaderSchema()));

    return new QueryResult(rowResults, queryRequest.getColumnHeaderSchema());
  }

  @Override
  public String executeAndExportResultsToGcs(QueryRequest queryRequest, String gcsBucketName) {
    // TODO: Add study and dataset names.
    String fileName =
        "tanagra_export_dataset_"
            + System.currentTimeMillis()
            // GCS file name must be in wildcard format.
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#export_option_list:~:text=The%20uri%20option%20must%20be%20a%20single%2Dwildcard%20URI
            + "_*.csv";

    String sql =
        String.format(
            "EXPORT DATA OPTIONS(uri='gs://%s/%s',format='CSV',overwrite=true,header=true) AS %n%s",
            gcsBucketName, fileName, queryRequest.getSql());
    LOGGER.info("Running SQL against BigQuery: {}", sql);
    bigQuery.queryBigQuery(sql);

    // Multiple files will be created only if export is very large (> 1GB). For now, just assume
    // only "000000000000" was created.
    // TODO: Detect and handle case where mulitple files are created.
    return fileName.replace("*", "000000000000");
  }
}
