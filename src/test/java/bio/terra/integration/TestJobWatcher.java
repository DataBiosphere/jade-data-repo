package bio.terra.integration;

import bio.terra.common.configuration.TestConfiguration;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.springframework.stereotype.Component;

/**
 * This can be used as a Rule for JUnit 4 integration tests. For JUnit 5 integration tests, see
 * TestJobWatcherExtension instead. It decorates failures with a link to GCP logs to make them
 * easier to debug.
 */
@Component
public class TestJobWatcher extends TestWatcher {
  private final TestConfiguration testConfig;

  public TestJobWatcher(TestConfiguration testConfig) {
    this.testConfig = testConfig;
  }

  @Override
  protected void failed(final Throwable e, final Description description) {
    TestJobWatcherUtils.emitLinkToGcpLogs(testConfig);
  }
}
