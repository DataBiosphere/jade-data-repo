package bio.terra.tanagra.query;

import com.google.cloud.bigquery.TableId;

public interface QueryExecutor {
  /** Execute a query request, returning the results of the query. */
  QueryResult execute(QueryRequest queryRequest);

  void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun);
}
