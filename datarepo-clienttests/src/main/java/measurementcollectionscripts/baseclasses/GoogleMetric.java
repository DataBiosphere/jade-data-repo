package measurementcollectionscripts.baseclasses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import java.io.IOException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.MeasurementCollectionScript;
import utils.MetricsUtils;

public class GoogleMetric extends MeasurementCollectionScript<TimeSeries> {
  private static final Logger logger = LoggerFactory.getLogger(GoogleMetric.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public GoogleMetric() {}

  protected String identifier;
  protected Aggregation aggregation;

  /** Download the raw data points generated during this test run. */
  public void downloadDataPoints(long startTimeMS, long endTimeMS) throws IOException {
    // send the request to the metrics server
    String filter =
        "metric.type=\""
            + identifier
            + "\" AND "
            + MetricsUtils.buildContainerAndNamespaceFilter(server);
    TimeInterval interval = MetricsUtils.buildTimeInterval(startTimeMS, endTimeMS);
    dataPoints =
        MetricsUtils.downloadTimeSeriesDataPoints(
            ProjectName.of(server.project), filter, interval, aggregation);

    // instantiate summary object
    summary = new MeasurementResultSummary(identifier);
  }

  /** Process the data points calculating reporting statistics of interest. */
  public void calculateSummaryStatistics() {
    // expect a non-zero number of data points
    if (dataPoints.size() == 0) {
      logger.error("No data points returned from metrics download: {}", identifier);
    }
    // downloading >1 TimeSeries is not wrong, and we may well want to persist >1 for additional
    // detail but the calculation below may makes more sense when there's only one TimeSeries
    else if (dataPoints.size() != 1) {
      logger.warn("More than one time series returned from metrics download: {}", identifier);
    }

    // calculate statistics across all data points in all time series
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (TimeSeries ts : dataPoints) {
      ts.getPointsList().stream()
          .forEach(pt -> descriptiveStatistics.addValue(pt.getValue().getDoubleValue()));
    }
    calculateStandardStatistics(descriptiveStatistics);
  }

  /** Write the raw data points generated during this test run to a String. */
  public String writeDataPointsToString() {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();

    // tell the mapper object to use the custom serializer defined below for the TimeSeries class
    SimpleModule simpleModule = new SimpleModule("TimeSeriesModule", new Version(1, 0, 0, null));
    simpleModule.addSerializer(new TimeSeriesSerializer());
    objectMapper.registerModule(simpleModule);

    // write the data points as a string
    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dataPoints);
    } catch (JsonProcessingException jpEx) {
      logger.error("Error serializing time series to JSON. {}", dataPoints, jpEx);
      return "Error serializing time series to JSON.";
    }
  }

  public static class TimeSeriesSerializer extends JsonSerializer<TimeSeries> {
    @Override
    public void serialize(TimeSeries value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();

      // TimeSeries.metric_kind and value_type properties
      jgen.writeStringField("metric_kind", value.getMetricKind().name());
      jgen.writeStringField("value_type", value.getValueType().name());

      // TimeSeries.metric nested object
      jgen.writeObjectFieldStart("metric");
      jgen.writeStringField("type", value.getMetric().getType());
      jgen.writeObjectField("labels", value.getMetric().getLabelsMap());
      jgen.writeEndObject();

      // TimeSeries.resource nested object
      jgen.writeObjectFieldStart("resource");
      jgen.writeStringField("type", value.getResource().getType());
      jgen.writeObjectField("labels", value.getResource().getLabelsMap());
      jgen.writeEndObject();

      // TimeSeries.points nested array of objects
      jgen.writeArrayFieldStart("points");
      for (Point pt : value.getPointsList()) {
        jgen.writeStartObject();

        // Point.interval nested object
        jgen.writeObjectFieldStart("interval");
        jgen.writeStringField("start_time", pt.getInterval().getStartTime().toString());
        jgen.writeStringField("end_time", pt.getInterval().getEndTime().toString());
        jgen.writeEndObject();

        jgen.writeNumberField("value", pt.getValue().getDoubleValue());
        jgen.writeEndObject();
      }
      jgen.writeEndArray();

      jgen.writeEndObject();
    }

    @Override
    public Class<TimeSeries> handledType() {
      return TimeSeries.class;
    }
  }
}
