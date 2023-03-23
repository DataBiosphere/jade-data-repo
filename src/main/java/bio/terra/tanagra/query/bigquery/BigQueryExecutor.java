package bio.terra.tanagra.query.bigquery;

import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.utils.GoogleBigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Iterables;
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
  public void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun) {
    throw new NotImplementedException();
  }
}
