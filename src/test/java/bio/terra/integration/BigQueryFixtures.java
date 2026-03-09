package bio.terra.integration;

import static org.junit.Assert.assertTrue;

import bio.terra.common.AclUtils;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public final class BigQueryFixtures {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryFixtures.class);

  private BigQueryFixtures() {}

  public static BigQuery getBigQuery(String projectId, Credentials credentials) {
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

    return BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(credentials)
        .setRetrySettings(retrySettings)
        .build()
        .getService();
  }

  public static BigQuery getBigQuery(String projectId, String token) {
    GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
    return getBigQuery(projectId, googleCredentials);
  }

  public static boolean datasetExists(BigQuery bigQuery, String projectId, String datasetName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.get(projectId);
    Dataset dataset =
        AclUtils.aclUpdateRetry(
            () -> {
              try {
                DatasetId datasetId = DatasetId.of(projectId, datasetName);
                return bigQuery.getDataset(datasetId);
              } catch (BigQueryException ex) {
                bigQueryProject.bigQueryAclUpdateShouldRetry(ex);
              }
              return null;
            });
    return dataset != null;
  }

  private static final int WAIT_FOR_ACCESS_SECONDS = 180;
  private static final int WAIT_FOR_ACCESS_LOGGING_INTERVAL_SECONDS = 30;

  /**
   * Common method to use to wait for SAM to sync to a google group, asserting access is granted to
   * the user associated with the BigQuery instance.
   *
   * <p>At the beginning of 2023, google rolled out some changes to IAM propagation that make
   * detecting IAM propagation when google groups are involved tricky. At this time all access to
   * google cloud resources involves a google group. The problem seems to be that there are multiple
   * eventually consistent caches. If we test for access and get a denied answer, that information
   * is cached for an unknown period of time. There is some chance that a future request will fail
   * even though others succeed. So simple polling won't work and likely poisons the cache.
   *
   * <p>The new method is to wait for a fixed time then test access. Callers should then fail
   * immediately if access is still not available: a failure encountered when polling after the wait
   * is susceptible to the poisoned cache issue.
   *
   * <p><a
   * href="https://github.com/broadinstitute/workbench-libs/pull/1234/files#diff-f22cbe85519ed50c31f21ba7f347ed4cfb3615a564edd069b95ca79bdea56780R337">Reference
   * implementation</a>
   */
  public static void assertBqDatasetAccessible(
      BigQuery bigQuery, String dataProject, String bqDatasetName) throws Exception {
    int sleptSeconds = 0;
    while (sleptSeconds < WAIT_FOR_ACCESS_SECONDS) {
      logger.info(
          "Slept {} seconds prior to checking for BigQuery access at {} seconds",
          sleptSeconds,
          WAIT_FOR_ACCESS_SECONDS);
      TimeUnit.SECONDS.sleep(WAIT_FOR_ACCESS_LOGGING_INTERVAL_SECONDS);
      sleptSeconds += WAIT_FOR_ACCESS_LOGGING_INTERVAL_SECONDS;
    }
    logger.info("Slept {} seconds: checking for BigQuery access now", sleptSeconds);

    assertTrue(
        "BigQuery dataset exists and is accessible",
        BigQueryFixtures.datasetExists(bigQuery, dataProject, bqDatasetName));
  }

  // Fixture that should replicate BigQueryProject.getBQDataset, but allows providing a
  // BigQuery instance with a user token so you can test if a particular user has access
  // on the BigQuery instance
  public static Dataset getBQDataset(
      BigQuery providedBigQuery, String googleProjectId, String datasetBQName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.get(googleProjectId);
    return AclUtils.aclUpdateRetry(
        () -> {
          try {
            DatasetId datasetId = DatasetId.of(googleProjectId, datasetBQName);
            return providedBigQuery.getDataset(datasetId);
          } catch (BigQueryException ex) {
            bigQueryProject.bigQueryAclUpdateShouldRetry(ex);
          }
          return null;
        });
  }
}
