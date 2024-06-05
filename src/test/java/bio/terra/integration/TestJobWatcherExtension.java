package bio.terra.integration;

import static org.springframework.test.context.junit.jupiter.SpringExtension.getApplicationContext;

import bio.terra.common.configuration.TestConfiguration;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * This is an Extension for JUnit 5 integration tests. For JUnit 4 integration tests, use
 * TestJobWatcher as a Rule instead. It decorates failures with a link to GCP logs to make them
 * easier to debug.
 */
class TestJobWatcherExtension implements TestWatcher {
  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {
    TestConfiguration testConfig = getApplicationContext(context).getBean(TestConfiguration.class);
    TestJobWatcherUtils.emitLinkToGcpLogs(testConfig);
  }
}
