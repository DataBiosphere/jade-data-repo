package bio.terra.app.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component

/**
 * This class implements DISABLED performance logging. It is used when the "perftest" profile is NOT active.
 *
 * It provides no-op method stubs for the interface methods. There is still some overhead to calling these
 * methods in a production environment, but it is low because there is no actual work being done here. It's just
 * the overhead of calling a Java method and returning.
 */
public class PerformanceLoggerOFF implements PerformanceLogger {

    public static final boolean isPerformanceLoggingEnabled = false;

    private static Logger logger = LoggerFactory.getLogger(PerformanceLoggerOFF.class);

    @Autowired
    public PerformanceLoggerOFF() {
        logger.info("Performance logging OFF");
    }

    public void log(String jobId, String className, String operationName,
                    long elapsedTime, long integerCount, Serializable additionalInfo) { }

    public String timerStart() {
        return null;
    }

    public void timerStart(String timerId) { }

    public void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                               long integerCount, Serializable additionalInfo) { }
}
