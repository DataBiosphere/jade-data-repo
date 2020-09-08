package scripts.uploadscripts;

import collector.MeasurementCollectionScript;
import collector.MeasurementCollector;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import common.utils.BigQueryUtils;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestRunner;
import runner.config.TestConfiguration;
import uploader.UploadScript;

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
  public void uploadResults(Path outputDirectory) throws Exception {
    // get a BigQuery client object
    logger.info("BigQuery project_id:dataset_name: {}:{}", projectId, datasetName);
    BigQuery bigQueryClient = BigQueryUtils.getClient(projectId);

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
    InsertAllResponse response =
        bigQueryClient.insertAll(
            InsertAllRequest.newBuilder(tableId).addRow(buildTestRunRow(outputDirectory)).build());
    if (response.hasErrors()) {
      logger.info("hasErrors is true");
      // If any of the insertions failed, this lets you inspect the errors
      for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
        entry.getValue().forEach(bqe -> logger.info("bqerror: {}", bqe.toString()));
      }
    }

    // insert into testScriptResults

    // insert into measurementCollection
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
