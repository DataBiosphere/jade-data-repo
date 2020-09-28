package scripts.measurementcollectionscripts;

import com.google.logging.v2.LogEntry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleLog;

public class FlightCaughtExceptionLog extends GoogleLog {
  private static final Logger logger = LoggerFactory.getLogger(FlightCaughtExceptionLog.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public FlightCaughtExceptionLog() {
    super();
  }

  protected String exceptionText;

  /**
   * Setter for any parameters required by the measurement collection script. These parameters will
   * be set by the Measurement Collector based on the current Measurement List, and can be used by
   * the measurement collection script methods.
   *
   * @param parameters list of string parameters supplied by the measurement collection script
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 1) {
      throw new IllegalArgumentException("Must provide exception text in the parameters list");
    }
    exceptionText = parameters.get(0);

    additionalFilter =
        "textPayload:(\"bio.terra.stairway.Flight: Caught exception\" AND "
            + "\""
            + exceptionText
            + "\")";
    description = "Flight caught exception: " + exceptionText.substring(0, 20);
  }

  protected double extractNumericValueFromLogEntry(LogEntry logEntry) {
    return 1; // just count the number of log entries that match the filter
  }
}
