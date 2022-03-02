package bio.terra.integration;

import bio.terra.common.configuration.TestConfiguration;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

// This class can be used in a rule for integration test to decorate failures to make it easier to
// debug
@Component
public class TestJobWatcher extends TestWatcher {
  private static final Logger logger = LoggerFactory.getLogger(TestJobWatcher.class);

  private static final String QUERY_TEMPLATE =
      "resource.type=\"k8s_container\"\n"
          + "resource.labels.project_id=\"broad-jade-integration\"\n"
          + "resource.labels.location=\"us-central1\"\n"
          + "resource.labels.cluster_name=\"integration-master\"\n"
          + "resource.labels.namespace_name=\"integration-<intNumber>\"\n"
          + "labels.k8s-pod/component=\"integration-<intNumber>-jade-datarepo-api";

  private final TestConfiguration testConfig;

  public TestJobWatcher(TestConfiguration testConfig) {
    this.testConfig = testConfig;
  }

  @Override
  protected void failed(final Throwable e, final Description description) {
    if (testConfig.getIntegrationServerNumber() != null) {
      logger.error("See server log info at: {}", getStackdriverUrl());
    }
  }

  private String getStackdriverUrl() {
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
