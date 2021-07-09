package bio.terra.datarepo.app.logging;

import java.time.Duration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class implements DISABLED performance logging. It is used when the "perftest" profile is NOT
 * active.
 *
 * <p>It provides no-op method stubs for the interface methods. There is still some overhead to
 * calling these methods in a production environment, but it is low because there is no actual work
 * being done here. It's just the overhead of calling a Java method and returning.
 */
@Component("performanceLogger")
@Profile("!perftest")
public class PerformanceLoggerOFF implements PerformanceLogger {

  public PerformanceLoggerOFF() {}

  public boolean isEnabled() {
    return false;
  }

  public void log(
      String jobId,
      String className,
      String operationName,
      Duration elapsedTime,
      long integerCount,
      Object additionalInfo) {}

  public String timerStart() {
    return "";
  }

  public void timerStart(String timerId) {}

  public void timerEndAndLog(
      String timerId,
      String jobId,
      String className,
      String operationName,
      long integerCount,
      Object additionalInfo) {}
}
