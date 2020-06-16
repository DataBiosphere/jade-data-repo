package bio.terra.app.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Primary
@Profile({"perftest"})
@Component

/**
 * This class implements ENABLED performance logging. It is used when the "perftest" profile is active.
 *
 * It generates logs using the same slf4j logger used throughout the API code, but with a specific
 * format intended to be propagated from StackDriver to BigQuery and then parsed with SQL for further analysis.
 *
 * Any time there is a new type of information that we want to include in performance logs, there are 2 options:
 *   - Add the new field to the format string in this class and add optional arguments to the log methods that accept
 *     a value for this new field.
 *   - Specify the new information as a serializable object (e.g. String or property of a POJO) and pass it to the
 *     existing log methods with the additionalInfo parameter.
 *  New types of information that seem broadly useful should use the first option and those that will probably only
 *  be used in a small number of places should use the second option.
 */
public class PerformanceLoggerON implements PerformanceLogger {

    public static final boolean isPerformanceLoggingEnabled = true;

    private ObjectMapper objectMapper;

    private static Logger logger = LoggerFactory.getLogger(PerformanceLoggerON.class);
    private static final String PerformanceLogFormat =
        "TimestampUTC: {}, JobId: {}, Class: {}, Operation: {}, " +
        "ElapsedTime: {}, IntegerCount: {}, AdditionalInfo: {}";

    // Implementation choice note:
    // Using a thread local map for timers instead of a static concurrent hash map avoids locks when updating the map.
    // Thread local means we can't track timers across different threads, while a concurrent hash map would allow this.
    // Neither option allows tracking timers across different DR Managers. We could use SQL to calculate time durations
    // between start/end timestamps to address that use case.
    private static final ThreadLocal<Map<String, Instant>> startTimeMap =
        ThreadLocal.withInitial(() -> new HashMap<>());

    @Autowired
    public PerformanceLoggerON(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        logger.info("Performance logging ON");
    }

    public void log(String jobId, String className, String operationName,
                    long elapsedTime, long integerCount, Serializable additionalInfo) {
        // serialize the additional info object to JSON
        String additionalInfoStr = "";
        if (additionalInfo != null) {
            try {
                additionalInfoStr = objectMapper.writeValueAsString(additionalInfo);
            } catch (JsonProcessingException jsonEx) {
                // if we hit a serialization error with the additional information object, use an empty string
                // and log the error separately for debugging
                logger.info("Failure serializing additionalInfo to JSON. " + additionalInfo.toString());
                additionalInfoStr = "";
            }
        }

        logger.info(PerformanceLogFormat, currentTimestamp(), jobId, className, operationName,
            elapsedTime, integerCount, additionalInfoStr);
    }

    /**
     * Generate a UTC timestamp for the current instant.
     */
    private String currentTimestamp() {
        return Instant.now().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    public String timerStart() {
        // generate a timer reference id to use for tracking
        String timerId = UUID.randomUUID().toString();

        timerStart(timerId);

        // return the id to the user, so they can pass it to the timerEndAndLog method
        return timerId;
    }

    public void timerStart(String timerId) {
        // if the timer reference id already exists, overwrite it and log the error separately for debugging
        if (startTimeMap.get().containsKey(timerId)) {
            logger.info("Overwriting existing performance timer entry. " + timerId);
        }

        // store the timer reference id in a thread local map for lookup later in the timerEndAndLog method
        Instant startTime = Instant.now();
        startTimeMap.get().put(timerId, startTime);
    }

    public void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                               long integerCount, Serializable additionalInfo) {
        Instant endTime = Instant.now();

        // if the timer reference id is not found, then just log a timestamp for the event
        // and log the error separately for debugging
        Instant startTime = startTimeMap.get().remove(timerId);
        if (startTime == null) {
            logger.info("Lookup of performance timer entry failed. " + timerId);
            log(jobId, className, operationName, 0, integerCount, additionalInfo);
            return;
        }

        // calculate the elapsed time and include it in the log
        long elapsedTimeNS = Duration.between(startTime, endTime).getNano();
        log(jobId, className, operationName, elapsedTimeNS, integerCount, additionalInfo);
    }
}
