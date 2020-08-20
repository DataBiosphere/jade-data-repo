package utils;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.logging.v2.LoggingClient;
import com.google.cloud.logging.v2.LoggingSettings;
import com.google.logging.v2.ListLogEntriesRequest;
import com.google.logging.v2.LogEntry;
import com.google.logging.v2.ProjectName;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

public class LogsUtils {
  private static final Logger logger = LoggerFactory.getLogger(LogsUtils.class);

  private static int minimumTimeRangeSizeInSeconds = 300; // test run has to last at least 5 minutes

  private LogsUtils() {}

  /**
   * Build a Google Logging client object with application default credentials. The client object is
   * newly created on each call to this method; it is not cached.
   */
  public static LoggingClient getClient() throws IOException {
    // LoggingOptions options = LoggingOptions.getDefaultInstance(); // v1 client api

    GoogleCredentials applicationDefaultCredentials =
        AuthenticationUtils.getApplicationDefaultCredential();
    LoggingSettings loggingServiceSettings =
        LoggingSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(applicationDefaultCredentials))
            .build();
    LoggingClient loggingServiceClient = LoggingClient.create(loggingServiceSettings);
    return loggingServiceClient;
  }

  /** Download the raw logging data points. */
  public static List<LogEntry> downloadLogEntryDataPoints(ProjectName project, String filter)
      throws IOException {
    LoggingClient loggingServiceClient = getClient();

    // Page<LogEntry> entries = loggingClient.listLogEntries(
    // Logging.EntryListOption.filter(filter)); // v1 client api

    ListLogEntriesRequest request =
        ListLogEntriesRequest.newBuilder()
            .addResourceNames(project.toString())
            .setFilter(filter)
            .build();
    LoggingClient.ListLogEntriesPagedResponse response =
        loggingServiceClient.listLogEntries(request);

    List<LogEntry> dataPoints = new ArrayList<>();
    response.iterateAll().forEach(dataPoints::add);

    return dataPoints;
  }

  /** Build a logging filter on the container name and namespace */
  public static String buildStdoutContainerAndNamespaceFilter(
      ServerSpecification server, long startTimeMS, long endTimeMS) {
    // test run should set the start/end time before/after the user journey threads run
    if (startTimeMS == -1 || endTimeMS == -1) {
      throw new RuntimeException("Start/end time was not set for this test run.");
    }

    // make sure interval is at least __ seconds long
    long minIntervalMS = minimumTimeRangeSizeInSeconds * 1000;
    if (endTimeMS - startTimeMS < minIntervalMS) {
      logger.info(
          "Test run lasted less than {} seconds. Expanding logs interval to include the {} seconds before the test run start.",
          minimumTimeRangeSizeInSeconds,
          minimumTimeRangeSizeInSeconds);
      startTimeMS -= minIntervalMS;
    }

    // convert start/end time from milliseconds to ISO-8601 format timestamp
    DateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted Z to indicate UTC
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    return "logName="
        + ProjectName.of(server.project).toString()
        + "/logs/stdout"
        + " AND resource.type=\"k8s_container\""
        + " AND resource.labels.project_id=\""
        + server.project
        + "\""
        + " AND resource.labels.location=\""
        + server.region
        + "\""
        + " AND resource.labels.cluster_name=\""
        + server.clusterShortName
        + "\""
        + " AND resource.labels.namespace_name=\""
        + server.namespace
        + "\""
        + " AND labels.k8s-pod/component=\""
        + server.containerName
        + "\""
        + " AND timestamp>=\""
        + dateFormat.format(new Date(startTimeMS))
        + "\""
        + " AND timestamp<=\""
        + dateFormat.format(new Date(endTimeMS))
        + "\"";
  }
}
