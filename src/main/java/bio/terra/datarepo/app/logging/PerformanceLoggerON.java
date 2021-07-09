package bio.terra.app.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class implements ENABLED performance logging. It is used when the "perftest" profile is
 * active.
 *
 * <p>It generates logs using the same slf4j logger used throughout the API code, but with a
 * specific format intended to be propagated from StackDriver to BigQuery and then parsed with SQL
 * for further analysis.
 *
 * <p>Any time there is a new type of information that we want to include in performance logs, there
 * are 2 options: - Add the new field to the format string in this class and add optional arguments
 * to the log methods that accept a value for this new field. - Specify the new information as an
 * object (e.g. String or property of a POJO) and pass it to the existing log methods with the
 * additionalInfo parameter. New types of information that seem broadly useful should use the first
 * option and those that will probably only be used in a small number of places should use the
 * second option.
 */
@Component("performanceLogger")
@Profile("perftest")
@Primary
public class PerformanceLoggerON implements PerformanceLogger {

  private ObjectMapper objectMapper;

  private static Logger logger = LoggerFactory.getLogger(PerformanceLoggerON.class);
  private static final String PerformanceLogFormat =
      "TimestampUTC: {}, JobId: {}, Class: {}, Operation: {}, "
          + "ElapsedTime: {}, IntegerCount: {}, AdditionalInfo: {}";

  // Implementation choice note:
  // Using a thread local map for timers instead of a static concurrent hash map avoids locks when
  // updating the map.
  // Thread local means we can't track timers across different threads, while a concurrent hash map
  // would allow this.
  // Neither option allows tracking timers across different DR Managers. We could use SQL to
  // calculate time durations
  // between start/end timestamps to address that use case.
  private static final ThreadLocal<Map<String, Instant>> startTimeMap =
      ThreadLocal.withInitial(() -> new HashMap<>());

  private static final ThreadLocal<Long> logCounter =
      ThreadLocal.withInitial(() -> Long.valueOf(0));

  public PerformanceLoggerON() {
    this.objectMapper = new ObjectMapper();
  }

  public boolean isEnabled() {
    return true;
  }

  public void log(
      String jobId,
      String className,
      String operationName,
      Duration elapsedTime,
      long integerCount,
      Object additionalInfo) {
    // serialize the additional info object to JSON
    String additionalInfoStr = "";
    if (additionalInfo != null) {
      try {
        additionalInfoStr = objectMapper.writeValueAsString(additionalInfo);
      } catch (JsonProcessingException jsonEx) {
        // if we hit a serialization error with the additional information object, log the string
        // anyway
        // and log the error separately for debugging
        logger.info("Failure serializing additionalInfo to JSON. " + additionalInfo.toString());
        additionalInfoStr = additionalInfo.toString();
      }
    }

    logger.info(
        PerformanceLogFormat,
        currentTimestamp(),
        jobId,
        className,
        operationName,
        elapsedTime.toString(),
        integerCount,
        additionalInfoStr);
  }

  /** Generate a UTC timestamp for the current instant. */
  private String currentTimestamp() {
    return Instant.now().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
  }

  public String timerStart() {
    // generate a timer reference id to use for tracking
    long logCounterVal = logCounter.get();
    String timerId = Thread.currentThread().getId() + "-" + logCounterVal;
    logCounter.set(logCounterVal == Long.MAX_VALUE ? 0 : logCounterVal + 1);

    timerStart(timerId);

    // return the id to the user, so they can pass it to the timerEndAndLog method
    return timerId;
  }

  public void timerStart(String timerId) {
    // if the timer reference id already exists, overwrite it and log the error separately for
    // debugging
    if (startTimeMap.get().containsKey(timerId)) {
      logger.info("Overwriting existing performance timer entry. " + timerId);
    }

    // store the timer reference id in a thread local map for lookup later in the timerEndAndLog
    // method
    Instant startTime = Instant.now();
    startTimeMap.get().put(timerId, startTime);
  }

  public void timerEndAndLog(
      String timerId,
      String jobId,
      String className,
      String operationName,
      long integerCount,
      Object additionalInfo) {
    Instant endTime = Instant.now();

    // if the timer reference id is not found, then just log a timestamp for the event
    // and log the error separately for debugging
    Instant startTime = startTimeMap.get().remove(timerId);
    if (startTime == null) {
      logger.info("Lookup of performance timer entry failed. " + timerId);
      log(jobId, className, operationName, Duration.ZERO, integerCount, additionalInfo);
      return;
    }

    // calculate the elapsed time and include it in the log
    Duration elapsedTime = Duration.between(startTime, endTime);
    log(jobId, className, operationName, elapsedTime, integerCount, additionalInfo);
  }
}
