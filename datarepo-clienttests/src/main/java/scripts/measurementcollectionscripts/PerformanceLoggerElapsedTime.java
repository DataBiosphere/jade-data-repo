package scripts.measurementcollectionscripts;

import com.google.logging.v2.LogEntry;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleLog;

public class PerformanceLoggerElapsedTime extends GoogleLog {
  private static final Logger logger = LoggerFactory.getLogger(PerformanceLoggerElapsedTime.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public PerformanceLoggerElapsedTime() {
    super();
  }

  protected String className;
  protected String operationName;

  /**
   * Setter for any parameters required by the measurement collection script. These parameters will
   * be set by the Measurement Collector based on the current Measurement List, and can be used by
   * the measurement collection script methods.
   *
   * @param parameters list of string parameters supplied by the measurement collection script
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 2) {
      throw new IllegalArgumentException(
          "Must provide class and operation names in the parameters list");
    }
    className = parameters.get(0);
    operationName = parameters.get(1);

    additionalFilter =
        "textPayload:(\"Class: " + className + ", Operation: " + operationName + "\")";
    description = "PerformanceLogger elapsed time: " + operationName;
  }

  protected double extractNumericValueFromLogEntry(LogEntry logEntry) {
    String textPayload = logEntry.getTextPayload();
    Pattern pattern = Pattern.compile("ElapsedTime: ([^,]*?),");
    Matcher matcher = pattern.matcher(textPayload);

    if (matcher.find()) {
      String elapsedTimeStr =
          matcher.group(1); // first match is the whole regex, index 1 is the first group
      Duration elapsedTimeDuration = Duration.parse(elapsedTimeStr);
      return elapsedTimeDuration.toMillis();
    } else {
      logger.error("Error parsing elapsed time from PerformanceLogger text payload");
      return 0;
    }
  }
}
