package collector;

import collector.config.MeasurementCollectionScriptSpecification;
import collector.config.MeasurementList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import common.utils.FileUtils;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestRunner;
import runner.config.ServerSpecification;
import runner.config.TestConfiguration;

public class MeasurementCollector {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementCollector.class);

  private MeasurementList measurementList;
  private List<MeasurementCollectionScript.MeasurementResultSummary> summaries;

  private ServerSpecification server;
  private long startTime = -1;
  private long endTime = -1;

  protected MeasurementCollector(
      MeasurementList measurementList, ServerSpecification server, long startTime, long endTime) {
    this.measurementList = measurementList;
    this.summaries = new ArrayList<>();

    this.server = server;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  private static final String measurementDataPointsFileName = "RAWDATA_measurementDataPoints.json";
  private static final String measurementSummariesFileName = "SUMMARY_measurementCollection.json";

  protected void executeMeasurementList(Path outputDirectory) throws Exception {
    // loop through the measurement script specifications
    for (MeasurementCollectionScriptSpecification specification :
        measurementList.measurementCollectionScripts) {
      // setup an instance of each measurement script class
      MeasurementCollectionScript script = specification.scriptClassInstance();
      script.initialize(server, specification.description, specification.saveRawDataPoints);
      script.setParameters(specification.parameters);

      // download raw data points and process them
      logger.info("Executing measurement collection script: {}", specification.description);
      script.processDataPoints(startTime, endTime);
      summaries.add(script.getSummaryStatistics());

      // write the full set of measurement data points to a file
      if (specification.saveRawDataPoints) {
        File measurementDataPointsFile =
            outputDirectory
                .resolve(
                    measurementDataPointsFileName.replace(
                        ".json", "_[" + script.description + "].json"))
                .toFile();
        script.writeRawDataPointsToFile(measurementDataPointsFile);
        logger.info(
            "All measurement data points from {} written to file: {}",
            specification.name,
            measurementDataPointsFile.getName());
      }
    }

    // write the measurement summaries to info
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
    logger.info(objectWriter.writeValueAsString(summaries));

    // write the measurement summaries to a file
    File measurementSummariesFile =
        FileUtils.createNewFile(outputDirectory.resolve(measurementSummariesFileName).toFile());
    objectWriter.writeValue(measurementSummariesFile, summaries);
    logger.info("Measurement summaries written to file: {}", measurementSummariesFile.getName());
  }

  /**
   * Read in the measurement collection summaries from the output directory and return the array of
   * MeasurementCollectionScript.MeasurementResultSummary Java objects.
   */
  public static MeasurementCollectionScript.MeasurementResultSummary[]
      getMeasurementCollectionSummaries(Path outputDirectory) throws Exception {
    return FileUtils.readOutputFileIntoJavaObject(
        outputDirectory,
        MeasurementCollector.measurementSummariesFileName,
        MeasurementCollectionScript.MeasurementResultSummary[].class);
  }

  public static void collectMeasurements(
      String measurementListFileName,
      String outputDirName,
      String serverFileName,
      long startTime,
      long endTime)
      throws Exception {
    // read in measurement list and validate it
    MeasurementList measurementList = MeasurementList.fromJSONFile(measurementListFileName);
    measurementList.validate();

    // read in server specification and validate it
    ServerSpecification server = ServerSpecification.fromJSONFile(serverFileName);
    server.validate();

    // create the output directory if it doesn't already exist
    Path outputDirectory = Paths.get(outputDirName);
    File outputDirectoryFile = outputDirectory.toFile();
    if (!outputDirectoryFile.exists()) {
      boolean outputDirectoryCreated = outputDirectoryFile.mkdirs();
      logger.debug(
          "outputDirectoryCreated {}: {}",
          outputDirectoryFile.getAbsolutePath(),
          outputDirectoryCreated);
    }
    logger.info(
        "Measurements collected will be written to directory: {}",
        outputDirectoryFile.getAbsolutePath());

    // get an instance of a collector and tell it to execute the measurement list
    MeasurementCollector collector =
        new MeasurementCollector(measurementList, server, startTime, endTime);
    collector.executeMeasurementList(outputDirectory);
  }

  public static void collectMeasurements(
      String measurementListFileName,
      String outputDirName,
      String serverFileName,
      String startTimestamp,
      String endTimestamp)
      throws Exception {
    // timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff] (timezone is UTC)
    TimeZone originalDefaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    long startTimeMS = Timestamp.valueOf(startTimestamp).getTime();
    long endTimeMS = Timestamp.valueOf(endTimestamp).getTime();
    TimeZone.setDefault(originalDefaultTimeZone);

    collectMeasurements(
        measurementListFileName, outputDirName, serverFileName, startTimeMS, endTimeMS);
  }

  public static void collectMeasurementsForTestRun(
      String measurementListFileName, String outputDirName) throws Exception {
    // build a list of output directories to collect measurements for
    List<Path> testRunOutputDirectories =
        TestRunner.getTestRunOutputDirectories(Paths.get(outputDirName));

    // loop through each test run output directory, collecting measurements separately for each one
    for (int ctr = 0; ctr < testRunOutputDirectories.size(); ctr++) {
      Path testRunOutputDirectory = testRunOutputDirectories.get(ctr);
      logger.info("==== UPLOADING RESULTS FROM TEST CONFIGURATION ({}) ====", ctr);

      // read in the test config and test run summary files
      TestConfiguration renderedTestConfig =
          TestRunner.getRenderedTestConfiguration(testRunOutputDirectory);
      TestRunner.TestRunSummary testRunSummary =
          TestRunner.getTestRunSummary(testRunOutputDirectory);

      if (renderedTestConfig.server.skipKubernetes) {
        logger.warn("The skipKubernetes flag is true, so there may be no measurements to collect.");
      }
      logger.info(
          "Test run id: {}, configuration: {}, server: {}",
          testRunSummary.id,
          renderedTestConfig.name,
          renderedTestConfig.server.name);

      MeasurementCollector.collectMeasurements(
          measurementListFileName,
          testRunOutputDirectory.toString(),
          renderedTestConfig.serverSpecificationFile,
          testRunSummary.startUserJourneyTime,
          testRunSummary.endUserJourneyTime);
    }
  }
}
