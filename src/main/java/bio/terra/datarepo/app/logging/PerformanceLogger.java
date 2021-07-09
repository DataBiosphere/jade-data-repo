package bio.terra.datarepo.app.logging;

import java.time.Duration;

/**
 * This class specifies the interface for performance logging. The implementation is selected based
 * on the active Spring profiles.
 */
public interface PerformanceLogger {

  /** Returns true if performance logging is enabled, false otherwise. */
  boolean isEnabled();

  /** Log a timestamp for an event, specified with a job id, class and operation names. */
  default void log(String jobId, String className, String operationName) {
    log(jobId, className, operationName, Duration.ZERO, 0, null);
  }

  /** Log a timestamp, elapsed time and integer count for an event. */
  default void log(
      String jobId,
      String className,
      String operationName,
      Duration elapsedTime,
      long integerCount) {
    log(jobId, className, operationName, elapsedTime, integerCount, null);
  }

  /**
   * Log a timestamp, elapsed time, integer count, and any additional information for an event. The
   * additional information is specified as any object (e.g. String, properties of a POJO). The
   * String representation of this object will be serialized into JSON before logging.
   */
  void log(
      String jobId,
      String className,
      String operationName,
      Duration elapsedTime,
      long integerCount,
      Object additionalInfo);

  /**
   * Start a timer for an event and generate an id to reference the timer. The timer id returned by
   * this method should be passed to the timerEndAndLog method when ending the timer. Example:
   * String timerId = performanceLogger.timerStart(); ...event that we want to time happens...
   * performanceLogger.timerEndAndLog(timerId, jobId, className, operationName); This method is
   * intended for timing events that happen in a single code context (e.g. file copy, long-running
   * loop).
   *
   * @return the generated timer reference id
   */
  String timerStart();

  /**
   * Start a timer for an event and use the specified id to reference the timer. The same timer id
   * passed to this method should also be passed to the timerEndAndLog method when ending the timer.
   * Example: performanceLogger.timerStart(timerId); ...event that we want to time happens...
   * performanceLogger.timerEndAndLog(timerId, jobId, className, operationName); This method is
   * intended for timing events that span multiple code contexts that already have a convenient
   * identifier (e.g. stairway flights, steps).
   *
   * @param timerId the user-specified timer reference id
   */
  void timerStart(String timerId);

  /**
   * End a timer for an event, given the timer reference id. Log a timestamp, the time duration, and
   * event information (job id, class and operation names).
   *
   * @param timerId the reference timer id
   */
  default void timerEndAndLog(
      String timerId, String jobId, String className, String operationName) {
    timerEndAndLog(timerId, jobId, className, operationName, 0, null);
  }

  /**
   * End a timer for an event, given the timer reference id. Log a timestamp, the time duration,
   * event information (job id, class and operation names), and an integer count.
   *
   * @param timerId the timer reference id
   */
  default void timerEndAndLog(
      String timerId, String jobId, String className, String operationName, long integerCount) {
    timerEndAndLog(timerId, jobId, className, operationName, integerCount, null);
  }

  /**
   * End a timer for an event, given the timer reference id. Log a timestamp, the time duration,
   * event information (job id, class and operation names), an integer count, and any additional
   * information for an event. The additional information is specified as any object (e.g. String,
   * properties of a POJO). The String representation of this object will be serialized into JSON
   * before logging.
   *
   * @param timerId the timer reference id
   */
  void timerEndAndLog(
      String timerId,
      String jobId,
      String className,
      String operationName,
      long integerCount,
      Object additionalInfo);
}
