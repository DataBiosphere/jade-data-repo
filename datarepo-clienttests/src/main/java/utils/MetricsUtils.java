package utils;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

public class MetricsUtils {
  private static final Logger logger = LoggerFactory.getLogger(MetricsUtils.class);

  private MetricsUtils() {}

  /**
   * Build a Google Metrics client object with application default credentials. The client object is
   * newly created on each call to this method; it is not cached.
   */
  public static MetricServiceClient getClient() throws IOException {
    GoogleCredentials applicationDefaultCredentials =
        AuthenticationUtils.getApplicationDefaultCredential();
    MetricServiceSettings metricServiceSettings =
        MetricServiceSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(applicationDefaultCredentials))
            .build();
    MetricServiceClient metricServiceClient = MetricServiceClient.create(metricServiceSettings);

    return metricServiceClient;
  }

  /** Download the raw metrics data points. */
  public static List<TimeSeries> downloadTimeSeriesDataPoints(
      ProjectName project, String filter, TimeInterval interval, Aggregation aggregation)
      throws IOException {
    MetricServiceClient metricServiceClient = getClient();

    ListTimeSeriesRequest.Builder requestBuilder =
        ListTimeSeriesRequest.newBuilder()
            .setName(project.toString())
            .setFilter(filter)
            .setInterval(interval);
    if (aggregation != null) {
      // only set aggregation if specified
      requestBuilder.setAggregation(aggregation);
    }
    ListTimeSeriesRequest request = requestBuilder.build();

    MetricServiceClient.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(request);

    List<TimeSeries> dataPoints = new ArrayList<>();
    response.iterateAll().forEach(dataPoints::add);

    return dataPoints;
  }

  /**
   * Build a TimeInterval object that spans the given duration. Force it to be at least 5 minutes
   * long.
   */
  public static TimeInterval buildTimeInterval(long startTimeMS, long endTimeMS) {
    // test run should set the start/end time before/after the user journey threads run
    if (startTimeMS == -1 || endTimeMS == -1) {
      throw new RuntimeException("Start/end time was not set for this test run.");
    }

    // make sure interval is at least 5 minutes long
    long fiveMinMS = 5 * 60 * 1000;
    if (endTimeMS - startTimeMS < fiveMinMS) {
      logger.info(
          "Test run lasted less than 5 minutes. Expanding metrics interval to include the 5 minutes before the test run start.");
      startTimeMS -= fiveMinMS;
    }

    // restrict time to duration of the test run
    TimeInterval interval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.fromMillis(startTimeMS))
            .setEndTime(Timestamps.fromMillis(endTimeMS))
            .build();

    return interval;
  }

  /** Build a metrics filter on the container name and namespace */
  public static String buildContainerAndNamespaceFilter(ServerSpecification server) {
    return "resource.labels.container_name=\""
        + server.containerName
        + "\" AND resource.labels.namespace_name=\""
        + server.namespace
        + "\"";
  }
}
