package bio.terra.integration;

import bio.terra.common.configuration.TestConfiguration;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

/**
 * Utility methods for reuse between Rules (JUnit 4) and Extensions (JUnit 5) which aide in
 * debugging tests.
 */
public class TestJobWatcherUtils {

  private static final Logger logger = LoggerFactory.getLogger(TestJobWatcherExtension.class);

  private static final String QUERY_TEMPLATE =
      """
          resource.type="k8s_container"
          resource.labels.project_id="broad-jade-integration"
          resource.labels.location="us-central1"
          resource.labels.cluster_name="integration-master"
          resource.labels.namespace_name="integration-<intNumber>"
          labels.k8s-pod/component="integration-<intNumber>-jade-datarepo-api""";

  /**
   * If these tests are running on an integration test server, emit a link to their recent logs in
   * the GCP log console.
   */
  public static void emitLinkToGcpLogs(TestConfiguration testConfig) {
    if (testConfig.getIntegrationServerNumber() != null) {
      logger.error("See server log info at: {}", getLinkToGcpLogs(testConfig));
    }
  }

  private static String getLinkToGcpLogs(TestConfiguration testConfig) {
    String query =
        URLEncoder.encode(
            new ST(QUERY_TEMPLATE)
                .add("intNumber", testConfig.getIntegrationServerNumber())
                .render(),
            StandardCharsets.UTF_8);
    return "https://console.cloud.google.com/logs/query;"
        + query
        + ";cursorTimestamp="
        + Instant.now().minus(Duration.ofSeconds(30)).toString()
        + "?project="
        + testConfig.getGoogleProjectId();
  }
}
