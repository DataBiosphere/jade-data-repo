package collector;

import collector.config.MeasurementCollectionScriptSpecification;
import collector.config.MeasurementList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import common.CommandCLI;
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
import runner.config.ServerSpecification;

public class MeasurementCollector {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementCollector.class);

  MeasurementList measurementList;
  List<MeasurementCollectionScript.MeasurementResultSummary> summaries;

  ServerSpecification server;
  public long startTime = -1;
  public long endTime = -1;

  MeasurementCollector(
      MeasurementList measurementList, ServerSpecification server, long startTime, long endTime) {
    this.measurementList = measurementList;
    this.summaries = new ArrayList<>();

    this.server = server;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  protected static final String measurementDataPointsFileName =
      "RAWDATA_measurementDataPoints.json";
  protected static final String measurementSummariesFileName = "SUMMARY_measurementCollection.json";

  public void executeMeasurementList(Path outputDirectory) throws Exception {
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
                        ".json", "_" + specification.name + ".json"))
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

  public static void main(String[] args) throws Exception {
    CommandCLI.collectMeasurementsMain(args);
  }
}
