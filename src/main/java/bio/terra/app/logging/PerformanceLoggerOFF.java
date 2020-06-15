package bio.terra.app.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class PerformanceLoggerOFF implements PerformanceLogger {
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
