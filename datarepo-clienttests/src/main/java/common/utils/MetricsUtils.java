package common.utils;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import runner.config.ServiceAccountSpecification;

public class MetricsUtils {
  private static final Logger logger = LoggerFactory.getLogger(MetricsUtils.class);

  // The log/metric timestamps are not exact and so trying to limit results to too small a window
  // might be misleading This parameter sets the minimum time interval size. If the interval
  // specified is less than this, then the code will expand the interval to the minimum size.
  private static int minimumTimeRangeSizeInSeconds = 1;

  private MetricsUtils() {}

  /**
   * Build a Google Metrics client object with credentials for the given service account. The client
   * object is newly created on each call to this method; it is not cached.
   */
  public static MetricServiceClient getClientForServiceAccount(
      ServiceAccountSpecification serviceAccount) throws Exception {
    GoogleCredentials serviceAccountCredentials =
        AuthenticationUtils.getServiceAccountCredential(serviceAccount);
    MetricServiceSettings metricServiceSettings =
        MetricServiceSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(serviceAccountCredentials))
            .build();
    MetricServiceClient metricServiceClient = MetricServiceClient.create(metricServiceSettings);

    return metricServiceClient;
  }

  /** Request the raw metrics data points. */
  public static MetricServiceClient.ListTimeSeriesPagedResponse requestTimeSeriesDataPoints(
      MetricServiceClient metricServiceClient,
      ProjectName project,
      String filter,
      TimeInterval interval,
      Aggregation aggregation)
      throws Exception {
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

    return response;
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

    // make sure interval is at least __ seconds long
    long minIntervalMS = minimumTimeRangeSizeInSeconds * 1000;
    if (endTimeMS - startTimeMS < minIntervalMS) {
      logger.info(
          "Test run lasted less than {} seconds. Expanding metrics interval to include the {} seconds before the test run start.",
          minimumTimeRangeSizeInSeconds,
          minimumTimeRangeSizeInSeconds);
      startTimeMS -= minIntervalMS;
    }

    startTimeMS -= 1000; // round down a second
    endTimeMS += 1000; // round up a second

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
