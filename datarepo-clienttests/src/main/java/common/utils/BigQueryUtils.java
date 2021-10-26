package common.utils;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import runner.config.ServiceAccountSpecification;
import runner.config.TestUserSpecification;

public final class BigQueryUtils {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryUtils.class);

  private BigQueryUtils() {}

  /**
   * Build the Big Query client object for the given test user specification and project.
   *
   * @param testUser the test user whose credentials are supplied to the API client object
   * @param googleProjectId the project where BigQuery will run queries
   * @return the BigQuery client object for this user and data project
   */
  public static BigQuery getClientForTestUser(
      TestUserSpecification testUser, String googleProjectId) throws IOException {
    logger.debug(
        "Fetching credentials and building BigQuery client object for test user: {}",
        testUser.name);

    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(Duration.ofSeconds(1))
            .setMaxRetryDelay(Duration.ofSeconds(32))
            .setRetryDelayMultiplier(2.0)
            .setTotalTimeout(Duration.ofMinutes(7))
            .setInitialRpcTimeout(Duration.ofSeconds(50))
            .setRpcTimeoutMultiplier(1.0)
            .setMaxRpcTimeout(Duration.ofSeconds(50))
            .build();

    GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
    BigQuery bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(googleProjectId)
            .setCredentials(userCredential)
            .setRetrySettings(retrySettings)
            .build()
            .getService();

    return bigQuery;
  }

  /**
   * Build the Big Query client object for the given service account specification and project.
   *
   * @param serviceAccount the service account whose credentials are supplied to the API client
   *     object
   * @param googleProjectId the project where BigQuery will run queries
   * @return the BigQuery client object for this user and data project
   */
  public static BigQuery getClientForServiceAccount(
      ServiceAccountSpecification serviceAccount, String googleProjectId) throws IOException {
    logger.debug(
        "Fetching credentials and building BigQuery client object for service account: {}",
        serviceAccount.name);

    GoogleCredentials serviceAccountCredentials =
        AuthenticationUtils.getServiceAccountCredential(serviceAccount);
    BigQuery bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(googleProjectId)
            .setCredentials(serviceAccountCredentials)
            .build()
            .getService();

    return bigQuery;
  }

  /**
   * Execute a query with the given BigQuery client object.
   *
   * @param bigQueryClient the BigQuery client object
   * @param query the query to run
   * @return the result of the BQ query
   * @throws InterruptedException from the bigQuery.query() method
   */
  public static TableResult queryBigQuery(BigQuery bigQueryClient, String query)
      throws InterruptedException {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    return bigQueryClient.query(queryConfig);
  }

  /**
   * Execute a query to check if a row that matches fieldName = fieldValue already exists. Use the
   * given BigQuery client object.
   *
   * @return true if the row already exists, false otherwise
   */
  public static boolean checkRowExists(
      BigQuery bigQueryClient,
      String projectId,
      String datasetName,
      String tableName,
      String fieldName,
      String fieldValue)
      throws InterruptedException {
    String tableRef = String.format("`%s.%s.%s`", projectId, datasetName, tableName);
    String queryForRow =
        String.format(
            "SELECT 1 FROM %s WHERE %s = '%s' LIMIT %s", tableRef, fieldName, fieldValue, 1);
    logger.debug("queryForRow: {}", queryForRow);

    TableResult result = queryBigQuery(bigQueryClient, queryForRow);
    AtomicInteger numMatchedRows = new AtomicInteger();
    result.iterateAll().forEach(r -> numMatchedRows.getAndIncrement());
    if (numMatchedRows.get() > 0) {
      logger.debug(
          "Row already exists: {} = {}, Matches found: {}",
          fieldName,
          fieldValue,
          numMatchedRows.get());
    }

    return (numMatchedRows.get() > 0);
  }

  /**
   * Execute a streaming insert given the request. Loop through and print out any errors returned.
   */
  public static void insertAllIntoBigQuery(BigQuery bigQueryClient, InsertAllRequest request) {
    InsertAllResponse response = bigQueryClient.insertAll(request);
    if (response.hasErrors()) {
      logger.error(
          "hasErrors is true after inserting into the {}.{}.{} table",
          bigQueryClient.getOptions().getProjectId(),
          request.getTable().getDataset(),
          request.getTable().getTable());
      // If any of the insertions failed, this lets you inspect the errors
      for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
        entry.getValue().forEach(bqe -> logger.info("bqerror: {}", bqe.toString()));
      }
    }
    logger.info(
        "Successfully inserted to BigQuery table: {}.{}.{}",
        bigQueryClient.getOptions().getProjectId(),
        request.getTable().getDataset(),
        request.getTable().getTable());
  }

  public static String getDatasetName(String datasetName) {
    return "datarepo_" + datasetName;
  }

  public static String buildSelectQuery(
      String project, String datasetName, String tableName, String select, Long limit) {
    String tableRef = String.format("`%s.%s.%s`", project, datasetName, tableName);
    String sqlQuery = String.format("SELECT %s FROM %s LIMIT %s", select, tableRef, limit);
    return sqlQuery;
  }
}
