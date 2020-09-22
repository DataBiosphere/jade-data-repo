package scripts.measurementcollectionscripts;

import com.google.logging.v2.LogEntry;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleLog;

public class PerformanceLoggerCount extends GoogleLog {
  private static final Logger logger = LoggerFactory.getLogger(PerformanceLoggerElapsedTime.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public PerformanceLoggerCount() {
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
    description = "PerformanceLogger integer count: " + operationName;
  }

  protected double extractNumericValueFromLogEntry(LogEntry logEntry) {
    String textPayload = logEntry.getTextPayload();
    Pattern pattern = Pattern.compile("IntegerCount: ([^,]*?),");
    Matcher matcher = pattern.matcher(textPayload);

    if (matcher.find()) {
      String integerCountStr =
          matcher.group(1); // first match is the whole regex, index 1 is the first group
      int integerCount = Integer.parseInt(integerCountStr);
      return integerCount;
    } else {
      logger.error("Error parsing integer count from PerformanceLogger text payload");
      return 0;
    }
  }
}
