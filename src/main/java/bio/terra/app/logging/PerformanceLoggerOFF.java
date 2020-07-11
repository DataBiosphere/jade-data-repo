package bio.terra.app.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * This class implements DISABLED performance logging. It is used when the "perftest" profile is NOT active.
 *
 * It provides no-op method stubs for the interface methods. There is still some overhead to calling these
 * methods in a production environment, but it is low because there is no actual work being done here. It's just
 * the overhead of calling a Java method and returning.
 */
@Component
public class PerformanceLoggerOFF implements PerformanceLogger {

    private static Logger logger = LoggerFactory.getLogger(PerformanceLoggerOFF.class);

    public PerformanceLoggerOFF() {
        logger.info("Performance logging OFF");
    }

    public boolean isEnabled() {
        return false;
    }

    public void log(String jobId, String className, String operationName,
                    Duration elapsedTime, long integerCount, Object additionalInfo) { }

    public String timerStart() {
        return "";
    }

    public void timerStart(String timerId) { }

    public void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                               long integerCount, Object additionalInfo) { }
}
