package collector;

import collector.config.MeasurementCollectionScriptSpecification;
import collector.config.MeasurementList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import common.CommandCLI;
import common.utils.FileUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

public class MeasurementCollector {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementCollector.class);

  MeasurementList measurementList;
  List<MeasurementCollectionScript> scripts;
  List<MeasurementCollectionScript.MeasurementResultSummary> summaries;

  ServerSpecification server;
  public long startTime = -1;
  public long endTime = -1;

  MeasurementCollector(
      MeasurementList measurementList, ServerSpecification server, long startTime, long endTime) {
    this.measurementList = measurementList;
    this.scripts = new ArrayList<>();
    this.summaries = new ArrayList<>();

    this.server = server;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public void executeMeasurementList() throws Exception {
    // setup an instance of each measurement script class
    for (MeasurementCollectionScriptSpecification specification :
        measurementList.measurementCollectionScripts) {
      MeasurementCollectionScript script = specification.scriptClassInstance();
      script.initialize(server, specification.description, specification.saveRawDataPoints);
      script.setParameters(specification.parameters);
      scripts.add(script);
    }

    // loop through the measurement scripts, downloading raw data points and processing them
    for (MeasurementCollectionScript script : scripts) {
      script.processDataPoints(startTime, endTime);
      summaries.add(script.getSummaryStatistics());
    }
  }

  protected static final String measurementDataPointsFileName =
      "RAWDATA_measurementDataPoints.json";
  protected static final String measurementSummariesFileName = "SUMMARY_measurementCollection.json";

  void writeOutResults(String outputDirName) throws Exception {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();

    // print the summary results to info
    logger.info(objectWriter.writeValueAsString(summaries));

    // the output directory should already exist
    Path outputDirectory = Paths.get(outputDirName);
    File outputDirectoryFile = outputDirectory.toFile();
    if (!outputDirectoryFile.exists()) {
      throw new FileNotFoundException(
          "Output directory not found: " + outputDirectoryFile.getAbsolutePath());
    }
    boolean outputDirectoryCreated = outputDirectoryFile.mkdirs();
    logger.debug(
        "outputDirectoryCreated {}: {}",
        outputDirectoryFile.getAbsolutePath(),
        outputDirectoryCreated);
    logger.info(
        "Measurements collected written to directory: {}", outputDirectoryFile.getAbsolutePath());

    // create the output files if they don't already exist
    File measurementSummariesFile =
        FileUtils.createNewFile(outputDirectory.resolve(measurementSummariesFileName).toFile());

    // write the full set of measurement data points to a file
    for (MeasurementCollectionScriptSpecification specification :
        measurementList.measurementCollectionScripts) {
      if (!specification.saveRawDataPoints) {
        continue;
      }
      File measurementDataPointsFile =
          outputDirectory
              .resolve(
                  measurementDataPointsFileName.replace(
                      ".json", "_" + specification.name + ".json"))
              .toFile();
      specification.scriptClassInstance().writeRawDataPointsToFile(measurementDataPointsFile);
      logger.info(
          "All measurement data points from {} written to file: {}",
          specification.name,
          measurementDataPointsFile.getName());
    }

    // write the measurement summaries to a file
    objectWriter.writeValue(measurementSummariesFile, summaries);
    logger.info("Measurement summaries written to file: {}", measurementSummariesFile.getName());
  }

  public static void collectMeasurements(
      String measurementListFileName,
      String outputDirName,
      ServerSpecification server,
      String startTimestamp,
      String endTimestamp)
      throws Exception {
    // timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff] (timezone is UTC)
    TimeZone originalDefaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    long startTimeMS = Timestamp.valueOf(startTimestamp).getTime();
    long endTimeMS = Timestamp.valueOf(endTimestamp).getTime();
    TimeZone.setDefault(originalDefaultTimeZone);

    collectMeasurements(measurementListFileName, outputDirName, server, startTimeMS, endTimeMS);
  }

  public static void collectMeasurements(
      String measurementListFileName,
      String outputDirName,
      ServerSpecification server,
      long startTime,
      long endTime)
      throws Exception {
    // read in measurement list and validate it
    MeasurementList measurementList = MeasurementList.fromJSONFile(measurementListFileName);
    measurementList.validate();

    // get an instance of a collector and tell it to execute the measurement list
    MeasurementCollector collector =
        new MeasurementCollector(measurementList, server, startTime, endTime);
    Exception collectorEx = null;
    try {
      collector.executeMeasurementList();
    } catch (Exception ex) {
      collectorEx = ex; // save exception to display after printing the results
    }

    collector.writeOutResults(outputDirName);

    if (collectorEx != null) {
      logger.error("Measurement Collector threw an exception", collectorEx);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandCLI.collectMeasurementsMain(args);
  }
}
