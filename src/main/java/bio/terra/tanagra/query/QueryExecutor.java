package bio.terra.tanagra.query;

public interface QueryExecutor {
  /** Execute a query request, returning the results of the query. */
  QueryResult execute(QueryRequest queryRequest);

  /**
   * @return GCS file name (no bucket name)
   */
  String executeAndExportResultsToGcs(QueryRequest queryRequest, String gcsBucketName);
}
