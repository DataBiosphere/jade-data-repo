package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertTrue;

import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotModel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public final class BigQueryFixtures {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryFixtures.class);
  private static final int SAM_TIMEOUT_SECONDS = 400;
  private static final int WAIT_FOR_ACCESS_SECONDS = 180;

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

  public static boolean datasetExists(BigQuery bigQuery, String projectId, String datasetName) {
    try {
      DatasetId datasetId = DatasetId.of(projectId, datasetName);
      Dataset dataset = bigQuery.getDataset(datasetId);
      return (dataset != null);
    } catch (Exception ex) {
      throw new IllegalStateException("existence check failed for " + datasetName, ex);
    }
  }

  public static TableResult query(String sql, BigQuery bigQuery) {
    try {
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
      return bigQuery.query(queryConfig);
    } catch (InterruptedException | BigQueryException e) {
      e.printStackTrace(System.out);
      throw new IllegalStateException("Query failed", e);
    }
  }

  /**
   * Run a query in BigQuery with retries
   *
   * <p>The BigQuery query is in an exponential backoff loop so that it tolerates access failures
   * due to GCP IAM update propagation. See DR-875 and the document: <a
   * href="https://docs.google.com/document/d/18j1ldbbXn-5Zyji5pHjx3CEg-SRQan2P2olY_6gAPUA">IAM
   * Propagation Note </a>
   *
   * @param sql query string to execute
   * @param bigQuery authenticated BigQuery object to use
   * @return TableResult object returned from BigQuery
   */
  private static final int RETRY_INITIAL_INTERVAL_SECONDS = 2;

  private static final int RETRY_MAX_INTERVAL_SECONDS = 30;
  private static final int RETRY_MAX_SLEEP_SECONDS = 420;

  public static TableResult queryWithRetry(String sql, BigQuery bigQuery)
      throws InterruptedException {
    int sleptSeconds = 0;
    int sleepSeconds = RETRY_INITIAL_INTERVAL_SECONDS;
    while (true) {
      try {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        return bigQuery.query(queryConfig);
      } catch (BigQueryException ex) {
        logger.info(
            "Caught BQ exception: code="
                + ex.getCode()
                + " reason="
                + ex.getReason()
                + " msg="
                + ex.getMessage());
        if ((sleptSeconds < RETRY_MAX_SLEEP_SECONDS)
            && (ex.getCode() == 403)
            && StringUtils.equals(ex.getReason(), "accessDenied")) {

          TimeUnit.SECONDS.sleep(sleepSeconds);
          sleptSeconds += sleepSeconds;
          logger.info("Slept " + sleepSeconds + " total slept " + sleptSeconds);
          sleepSeconds = Math.min(2 * sleepSeconds, RETRY_MAX_INTERVAL_SECONDS);
        } else {
          throw ex;
        }
      }
    }
  }

  public static String makeTableRef(SnapshotModel snapshotModel, String tableName) {
    return String.format(
        "`%s.%s.%s`", snapshotModel.getDataProject(), snapshotModel.getName(), tableName);
  }

  public static String makeTableRef(DatasetModel datasetModel, String tableName) {
    return String.format(
        "`%s.%s.%s`",
        datasetModel.getDataProject(),
        PdaoConstant.PDAO_PREFIX + datasetModel.getName(),
        tableName);
  }

  // Given a dataset, table, and column, query for a DRS URI and extract the DRS Object Id
  private static final Pattern drsIdRegex = Pattern.compile("([^/]+)$");

  public static String queryForDrsId(
      BigQuery bigQuery, SnapshotModel snapshotModel, String tableName, String columnName)
      throws InterruptedException {
    waitBeforeCheckingAccess();

    String sql =
        String.format(
            "SELECT %s FROM %s WHERE %s IS NOT NULL LIMIT 1",
            columnName, makeTableRef(snapshotModel, tableName), columnName);
    TableResult ids = BigQueryFixtures.queryWithRetry(sql, bigQuery);
    assertThat("Got one row", ids.getTotalRows(), equalTo(1L));

    String drsUri = null;
    for (FieldValueList fieldValueList : ids.iterateAll()) {
      drsUri = fieldValueList.get(0).getStringValue();
    }
    assertThat("DRS URI was found", drsUri, notNullValue());

    Matcher matcher = drsIdRegex.matcher(drsUri);
    assertThat("matcher found a match in the DRS URI", matcher.find(), equalTo(true));
    return matcher.group();
  }

  // Common method to use to wait for SAM to sync to a google group, allowing access by the
  // user associated with the BigQuery instance.
  public static boolean hasAccess(BigQuery bigQuery, String dataProject, String bqDatasetName)
      throws Exception {
    waitBeforeCheckingAccess();

    return TestUtils.eventualExpect(
        5,
        SAM_TIMEOUT_SECONDS,
        true,
        () -> {
          try {
            boolean bqDatasetExists =
                BigQueryFixtures.datasetExists(bigQuery, dataProject, bqDatasetName);
            assertTrue("BigQuery dataset exists and is accessible", bqDatasetExists);
            return true;
          } catch (IllegalStateException e) {
            assertThat(
                "access is denied until SAM syncs the reader policy with Google",
                e.getCause().getMessage(),
                startsWith("Access Denied:"));
            return false;
          }
        });
  }

  /**
   * At the beginning of 2023, google rolled out some changes to IAM propagation that make detecting
   * IAM propagation when google groups are involved tricky. At this time all access to google cloud
   * resources involves a google group. The problem seems to be that there are multiple eventually
   * consistent caches. If we test for access and get a denied answer, that information is cached
   * for an unknown period of time. There is some chance that a future request will fail even though
   * others succeed. So simple polling won't work and likely poisons the cache.
   *
   * <p>The new method is to wait for a fixed time then test access. A possible future improvement
   * is to then fail immediately if access is still not available: a failure encountered when
   * polling after the wait is susceptible to the poisoned cache issue.
   *
   * <p>Reference implementation:
   * https://github.com/broadinstitute/workbench-libs/pull/1234/files#diff-f22cbe85519ed50c31f21ba7f347ed4cfb3615a564edd069b95ca79bdea56780R337
   *
   * @throws InterruptedException
   */
  private static void waitBeforeCheckingAccess() throws InterruptedException {
    logger.info("Sleeping " + WAIT_FOR_ACCESS_SECONDS + " prior to checking BigQuery access");
    TimeUnit.SECONDS.sleep(WAIT_FOR_ACCESS_SECONDS);
    logger.info("Slept " + WAIT_FOR_ACCESS_SECONDS + " prior to checking BigQuery access");
  }
}
