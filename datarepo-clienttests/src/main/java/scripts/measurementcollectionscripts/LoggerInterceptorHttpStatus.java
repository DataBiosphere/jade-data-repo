package scripts.measurementcollectionscripts;

import com.google.logging.v2.LogEntry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleLog;

public class LoggerInterceptorHttpStatus extends GoogleLog {
  private static final Logger logger = LoggerFactory.getLogger(LoggerInterceptorHttpStatus.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public LoggerInterceptorHttpStatus() {
    super();
  }

  protected int httpStatusCode;

  /**
   * Setter for any parameters required by the measurement collection script. These parameters will
   * be set by the Measurement Collector based on the current Measurement List, and can be used by
   * the measurement collection script methods.
   *
   * @param parameters list of string parameters supplied by the measurement collection script
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 1) {
      throw new IllegalArgumentException("Must provide HTTP status in the parameters list");
    }
    try {
      httpStatusCode = Integer.parseInt(parameters.get(0));
    } catch (NumberFormatException nfEx) {
      throw new RuntimeException("Error parsing HTTP status code: " + parameters.get(0), nfEx);
    }

    additionalFilter =
        "textPayload:(\"LoggerInterceptor\")"
            + " AND textPayload:(\"status: "
            + httpStatusCode
            + "\")";
    description = "LoggerInterceptor HTTP status: " + httpStatusCode;
  }

  protected double extractNumericValueFromLogEntry(LogEntry logEntry) {
    return 1; // just count the number of log entries that match the filter
  }
}
