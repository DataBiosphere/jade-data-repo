package scripts.uploadscripts;

import bio.terra.testrunner.collector.MeasurementCollectionScript;
import bio.terra.testrunner.collector.MeasurementCollector;
import bio.terra.testrunner.common.utils.BigQueryUtils;
import bio.terra.testrunner.runner.TestRunner;
import bio.terra.testrunner.runner.TestScriptResult;
import bio.terra.testrunner.runner.config.ServiceAccountSpecification;
import bio.terra.testrunner.runner.config.TestConfiguration;
import bio.terra.testrunner.runner.config.TestScriptSpecification;
import bio.terra.testrunner.uploader.UploadScript;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SummariesToBigQueryJade extends UploadScript {
  private static final Logger logger = LoggerFactory.getLogger(SummariesToBigQueryJade.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SummariesToBigQueryJade() {}

  protected String projectId; // google project id
  protected String datasetName; // big query dataset name

  protected TestConfiguration renderedTestConfiguration;
  protected TestRunner.TestRunSummary testRunSummary;
  protected MeasurementCollectionScript.MeasurementResultSummary[] measurementCollectionSummaries;

  /**
   * Setter for any parameters required by the upload script. These parameters will be set by the
   * Result Uploader based on the current Upload List, and can be used by the upload script methods.
   *
   * @param parameters list of string parameters supplied by the upload list
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 2) {
      throw new IllegalArgumentException(
          "Must provide BigQuery project_id and dataset_name in the parameters list");
    }
    projectId = parameters.get(0);
    datasetName = parameters.get(1);
  }

  private static String testRunTableName = "testRun";
  private static String testScriptResultsTableName = "testScriptResults";
  private static String measurementCollectionTableName = "measurementCollection";

  /**
   * Upload the test results saved to the given directory. Results may include Test Runner
   * client-side output and any relevant measurements collected.
   */
  public void uploadResults(
      Path outputDirectory, ServiceAccountSpecification uploaderServiceAccount) throws Exception {
    // get a BigQuery client object
    logger.debug("BigQuery project_id:dataset_name: {}:{}", projectId, datasetName);
    BigQuery bigQueryClient =
        BigQueryUtils.getClientForServiceAccount(uploaderServiceAccount, projectId);

    // read in TestConfiguration TestRunSummary, and array of
    // MeasurementCollectionScript.MeasurementResultSummary objects
    renderedTestConfiguration = TestRunner.getRenderedTestConfiguration(outputDirectory);
    testRunSummary = TestRunner.getTestRunSummary(outputDirectory);
    measurementCollectionSummaries =
        MeasurementCollector.getMeasurementCollectionSummaries(outputDirectory);

    // insert a single row into testRun
    if (BigQueryUtils.checkRowExists(
        bigQueryClient, projectId, datasetName, testRunTableName, "id", testRunSummary.id)) {
      logger.info(
          "A row with this id already exists in the "
              + testRunTableName
              + " table. Inserting a duplicate.");
    }
    TableId tableId = TableId.of(datasetName, testRunTableName);
    InsertAllRequest insertRequest =
        InsertAllRequest.newBuilder(tableId).addRow(buildTestRunRow(outputDirectory)).build();
    BigQueryUtils.insertAllIntoBigQuery(bigQueryClient, insertRequest);

    // insert into testScriptResults
    tableId = TableId.of(datasetName, testScriptResultsTableName);
    InsertAllRequest.Builder insertRequestBuilder = InsertAllRequest.newBuilder(tableId);
    for (int ctr = 0; ctr < renderedTestConfiguration.testScripts.size(); ctr++) {
      TestScriptSpecification testScriptSpecification =
          renderedTestConfiguration.testScripts.get(ctr);
      TestScriptResult.TestScriptResultSummary testScriptResult =
          testRunSummary.testScriptResultSummaries.get(ctr);
      insertRequestBuilder.addRow(
          buildTestScriptResultsRow(testScriptSpecification, testScriptResult));
    }
    BigQueryUtils.insertAllIntoBigQuery(bigQueryClient, insertRequestBuilder.build());

    // insert into measurementCollection
    if (measurementCollectionSummaries == null) {
      logger.info("No measurement summaries found.");
    } else {
      tableId = TableId.of(datasetName, measurementCollectionTableName);
      insertRequestBuilder = InsertAllRequest.newBuilder(tableId);
      for (MeasurementCollectionScript.MeasurementResultSummary measurementResult :
          measurementCollectionSummaries) {
        insertRequestBuilder.addRow(buildMeasurementCollectionRow(measurementResult));
      }
      BigQueryUtils.insertAllIntoBigQuery(bigQueryClient, insertRequestBuilder.build());
    }
  }

  /** Build a single row for each measurement collection. */
  private Map<String, Object> buildMeasurementCollectionRow(
      MeasurementCollectionScript.MeasurementResultSummary measurementResult) {
    Map<String, Object> rowContent = new HashMap<>();

    rowContent.put("testRun_id", testRunSummary.id);

    rowContent.put("description", measurementResult.description);

    rowContent.put("statistics_min", String.valueOf(measurementResult.statistics.min));
    rowContent.put("statistics_max", String.valueOf(measurementResult.statistics.max));
    rowContent.put("statistics_mean", String.valueOf(measurementResult.statistics.mean));
    rowContent.put(
        "statistics_standardDeviation",
        String.valueOf(measurementResult.statistics.standardDeviation));
    rowContent.put("statistics_median", String.valueOf(measurementResult.statistics.median));
    rowContent.put(
        "statistics_percentile95", String.valueOf(measurementResult.statistics.percentile95));
    rowContent.put(
        "statistics_percentile99", String.valueOf(measurementResult.statistics.percentile99));
    rowContent.put("statistics_sum", String.valueOf(measurementResult.statistics.sum));

    return rowContent;
  }

  /** Build a single row for each test script result. */
  private Map<String, Object> buildTestScriptResultsRow(
      TestScriptSpecification testScriptSpecification,
      TestScriptResult.TestScriptResultSummary testScriptResult) {
    Map<String, Object> rowContent = new HashMap<>();

    rowContent.put("testRun_id", testRunSummary.id);

    rowContent.put("name", testScriptSpecification.name);
    rowContent.put(
        "numberOfUserJourneyThreadsToRun", testScriptSpecification.numberOfUserJourneyThreadsToRun);
    rowContent.put("userJourneyThreadPoolSize", testScriptSpecification.userJourneyThreadPoolSize);
    rowContent.put("expectedTimeForEach", testScriptSpecification.expectedTimeForEach);
    rowContent.put("expectedTimeForEachUnit", testScriptSpecification.expectedTimeForEachUnit);
    rowContent.put("parameters", testScriptSpecification.parameters);

    rowContent.put("description", testScriptResult.testScriptDescription);
    rowContent.put("elapsedTime_min", testScriptResult.elapsedTimeStatistics.min);
    rowContent.put("elapsedTime_max", testScriptResult.elapsedTimeStatistics.max);
    rowContent.put("elapsedTime_mean", testScriptResult.elapsedTimeStatistics.mean);
    rowContent.put(
        "elapsedTime_standardDeviation", testScriptResult.elapsedTimeStatistics.standardDeviation);
    rowContent.put("elapsedTime_median", testScriptResult.elapsedTimeStatistics.median);
    rowContent.put("elapsedTime_percentile95", testScriptResult.elapsedTimeStatistics.percentile95);
    rowContent.put("elapsedTime_percentile99", testScriptResult.elapsedTimeStatistics.percentile99);
    rowContent.put("elapsedTime_sum", testScriptResult.elapsedTimeStatistics.sum);

    rowContent.put("totalRun", testScriptResult.totalRun);
    rowContent.put("numCompleted", testScriptResult.numCompleted);
    rowContent.put("numExceptionsThrown", testScriptResult.numExceptionsThrown);
    rowContent.put("isFailure", testScriptResult.isFailure);

    return rowContent;
  }

  /** Build a single row for each test run. */
  private Map<String, Object> buildTestRunRow(Path outputDirectory) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> rowContent = new HashMap<>();

    rowContent.put("id", testRunSummary.id);
    rowContent.put("testConfig_name", renderedTestConfiguration.name);
    rowContent.put("testConfig_description", renderedTestConfiguration.description);
    rowContent.put("server_name", renderedTestConfiguration.server.name);
    rowContent.put(
        "kubernetes_numberOfInitialPods", renderedTestConfiguration.kubernetes.numberOfInitialPods);
    rowContent.put(
        "testUsers", renderedTestConfiguration.testUsers.stream().map(tu -> tu.name).toArray());

    TimeZone originalDefaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    rowContent.put(
        "startTime", Timestamp.from(Instant.ofEpochMilli(testRunSummary.startTime)).toString());
    rowContent.put(
        "startUserJourneyTime",
        Timestamp.from(Instant.ofEpochMilli(testRunSummary.startUserJourneyTime)).toString());
    rowContent.put(
        "endUserJourneyTime",
        Timestamp.from(Instant.ofEpochMilli(testRunSummary.endUserJourneyTime)).toString());
    rowContent.put(
        "endTime", Timestamp.from(Instant.ofEpochMilli(testRunSummary.endTime)).toString());
    TimeZone.setDefault(originalDefaultTimeZone);

    rowContent.put("outputDirectory", outputDirectory.toAbsolutePath().toString());
    rowContent.put(
        "json_testConfiguration", objectMapper.writeValueAsString(renderedTestConfiguration));
    rowContent.put("json_testRun", objectMapper.writeValueAsString(testRunSummary));
    rowContent.put(
        "json_measurementCollection",
        objectMapper.writeValueAsString(measurementCollectionSummaries));

    return rowContent;
  }
}
