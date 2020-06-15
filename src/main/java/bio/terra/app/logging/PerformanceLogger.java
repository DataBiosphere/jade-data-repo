package bio.terra.app.logging;

import java.io.Serializable;

public interface PerformanceLogger {

    default void log(String jobId, String className, String operationName) {
        log(jobId, className, operationName, 0, 0, null);
    }

    default void log(String jobId, String className, String operationName,
                    long elapsedTime, long integerCount) {
        log(jobId, className, operationName, elapsedTime, integerCount, null);
    }

    void log(String jobId, String className, String operationName,
             long timeDurationMS, long integerCount, Serializable additionalInfo);

    String timerStart();

    void timerStart(String timerId);

    default void timerEndAndLog(String timerId, String jobId, String className, String operationName) {
        timerEndAndLog(timerId, jobId, className, operationName, 0, null);
    }

    default void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                               long integerCount) {
        timerEndAndLog(timerId, jobId, className, operationName, integerCount, null);
    }

    void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                        long integerCount, Serializable additionalInfo);
}
