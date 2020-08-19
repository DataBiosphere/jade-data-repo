package runner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import utils.MetricsUtils;

public class MetricResult {
  private static final Logger logger = LoggerFactory.getLogger(MetricResult.class);

  String identifier;
  Aggregation aggregation;
  List<TimeSeries> dataPoints;

  MetricResultSummary summary;

  @SuppressFBWarnings(
      value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
      justification = "This POJO class is used for easy serialization to JSON using Jackson.")
  public static class MetricResultSummary {
    public String description;

    public double min;
    public double max;
    public double mean;

    public boolean isFailure;

    private MetricResultSummary(String description) {
      this.description = description;
    }
  }

  public MetricResult(String identifier, Aggregation aggregation) {
    this.identifier = identifier;
    this.aggregation = aggregation;
  }

  public MetricResultSummary getSummary() {
    return summary;
  }

  public List<TimeSeries> getDataPoints() {
    return dataPoints;
  }

  /** Download the raw metrics data points generated for this server during this test run. */
  public void downloadDataPoints(ServerSpecification server, long startTimeMS, long endTimeMS)
      throws IOException {
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

    // calculate statistics for reporting
    summary = new MetricResultSummary(identifier);
    calculateStatistics();
  }

  /** Loop through the data points calculating reporting statistics of interest. */
  private void calculateStatistics() {
    // downloading >1 TimeSeries is not wrong, and we may well want to persist >1 for additional
    // detail
    // but the calculation below (I think) makes more sense when there's only one TimeSeries
    if (dataPoints.size() != 1) {
      logger.warn("More than one time series returned from metrics download: {}", identifier);
    }

    // calculate the min, max and mean of all data points across all time series
    long numDataPoints = 0;
    for (TimeSeries ts : dataPoints) {
      for (Point pt : ts.getPointsList()) {
        double dataPointVal = pt.getValue().getDoubleValue();
        summary.mean += dataPointVal;
        if (dataPointVal < summary.min || numDataPoints == 0) {
          summary.min = dataPointVal;
        }
        if (dataPointVal > summary.max || numDataPoints == 0) {
          summary.max = dataPointVal;
        }

        numDataPoints++;
      }
    }

    // expect a non-zero number of data points
    if (numDataPoints == 0) {
      logger.error("No data points returned from metrics download: {}", identifier);
      summary.min = 0;
      summary.max = 0;
      summary.mean = 0;
    } else {
      summary.mean /= numDataPoints;
    }

    // currently success means that we were able to download any metrics
    // TODO: do we need a way to specify a benchmark for each metric to determine whether the run
    // passed or not?
    summary.isFailure = (numDataPoints > 0);
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
