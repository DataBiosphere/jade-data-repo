package scripts.measurementcollectionscripts.baseclasses;

import collector.MeasurementCollectionScript;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.cloud.logging.v2.LoggingClient;
import com.google.logging.v2.LogEntry;
import com.google.logging.v2.ProjectName;
import common.BasicStatistics;
import common.utils.LogsUtils;
import java.io.File;
import java.io.IOException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleLog extends MeasurementCollectionScript<LogEntry> {
  private static final Logger logger = LoggerFactory.getLogger(GoogleLog.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public GoogleLog() {}

  protected String additionalFilter;
  protected DescriptiveStatistics descriptiveStatistics;

  /**
   * Download the raw data points generated during this test run. Then process them to calculate
   * reporting statistics of interest.
   */
  public void processDataPoints(long startTimeMS, long endTimeMS) throws Exception {
    // send the request to the logging server
    String filter =
        LogsUtils.buildStdoutContainerAndNamespaceFilter(server, startTimeMS, endTimeMS);
    if (additionalFilter != null && !additionalFilter.isEmpty()) {
      filter = filter + " AND " + additionalFilter;
    }
    logger.debug("filter: {}", filter);
    LoggingClient loggingClient =
        LogsUtils.getClientForServiceAccount(server.testRunnerServiceAccount);
    logger.debug(
        "Request Log Entries: {}; {}", server.project, server.testRunnerServiceAccount.name);
    LoggingClient.ListLogEntriesPagedResponse response =
        LogsUtils.requestLogEntries(loggingClient, ProjectName.of(server.project), filter, null);

    // iterate through all log entries returned, keeping either the whole entry or just the numeric
    // value
    descriptiveStatistics = new DescriptiveStatistics();
    response
        .iterateAll()
        .forEach(
            logEntry -> {
              descriptiveStatistics.addValue(extractNumericValueFromLogEntry(logEntry));
              if (saveRawDataPoints) {
                dataPoints.add(logEntry);
              }
            });

    // expect a non-zero number of data points
    if (descriptiveStatistics.getN() == 0) {
      logger.error("No data points returned from logs download: {}", description);
    }

    // calculate basic summary statistics on the numeric values of the log entries
    summary = new MeasurementResultSummary(description);
    summary.statistics = BasicStatistics.calculateStandardStatistics(descriptiveStatistics);
  }

  protected double extractNumericValueFromLogEntry(LogEntry logEntry) {
    throw new UnsupportedOperationException(
        "extractNumericValueFromLogEntry must be overridden by sub-classes");
  }

  /** Write the raw data points generated during this test run to a String. */
  public void writeRawDataPointsToFile(File outputFile) throws Exception {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();

    // tell the mapper object to use the custom serializer defined below for the TimeSeries class
    SimpleModule simpleModule = new SimpleModule("LogEntryModule", new Version(1, 0, 0, null));
    simpleModule.addSerializer(new LogEntrySerializer());
    objectMapper.registerModule(simpleModule);

    // write the data points as a string
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, dataPoints);
  }

  public static class LogEntrySerializer extends JsonSerializer<LogEntry> {
    @Override
    public void serialize(LogEntry value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();

      // LogEntry.timestamp nested object
      jgen.writeObjectFieldStart("timestamp");
      jgen.writeNumberField("seconds", value.getTimestamp().getSeconds());
      jgen.writeNumberField("nanos", value.getTimestamp().getNanos());
      jgen.writeEndObject();

      // LogEntry properties
      jgen.writeObjectField("json_payload", value.getJsonPayloadOrBuilder().getFieldsMap());
      jgen.writeStringField("text_payload", value.getTextPayload());
      jgen.writeStringField("insert_id", value.getInsertId());

      // set to false for debugging, to include more fields in LogEntry serialization
      boolean useSmallerSerialization = true;

      if (!useSmallerSerialization) {
        jgen.writeStringField("severity", value.getSeverity().name());
        jgen.writeStringField("log_name", value.getLogName());
        jgen.writeObjectField("labels", value.getLabelsMap());

        // LogEntry.receive_timestamp nested object
        jgen.writeObjectFieldStart("receive_timestamp");
        jgen.writeNumberField("seconds", value.getTimestamp().getSeconds());
        jgen.writeNumberField("nanos", value.getTimestamp().getNanos());
        jgen.writeEndObject();

        // LogEntry.resource nested object
        jgen.writeObjectFieldStart("resource");
        jgen.writeStringField("type", value.getResource().getType());
        jgen.writeObjectField("labels", value.getResource().getLabelsMap());
        jgen.writeEndObject();
      }

      jgen.writeEndObject();
    }

    @Override
    public Class<LogEntry> handledType() {
      return LogEntry.class;
    }
  }
}
