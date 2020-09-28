package collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.BasicStatistics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

public abstract class MeasurementCollectionScript<T> {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementCollectionScript.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public MeasurementCollectionScript() {}

  protected ServerSpecification server;
  protected String description;
  protected boolean saveRawDataPoints;
  protected MeasurementResultSummary summary;

  @SuppressFBWarnings(
      value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
      justification = "This POJO class is used for easy serialization to JSON using Jackson.")
  public static class MeasurementResultSummary {
    public String description;
    public BasicStatistics statistics;

    public MeasurementResultSummary() {}

    public MeasurementResultSummary(String description) {
      this.description = description;
    }
  }

  /**
   * Setter for properties of this class: server specification, description, flag whether to save
   * the raw result data. These properties will be set by the Measurement Collector based on the
   * current Measurement List, and can be accessed by the measurement collection script methods.
   *
   * @param server the specification of the server(s) where the measurements were collected
   * @param description the description of the measurement being collected
   * @param saveRawDataPoints true to save the raw results to the output file, false to just
   *     calculate the summary statistics
   */
  public void initialize(
      ServerSpecification server, String description, boolean saveRawDataPoints) {
    this.server = server;
    this.description = description;
    this.saveRawDataPoints = saveRawDataPoints;
    this.dataPoints = new ArrayList<>();
  }

  /**
   * Setter for any parameters required by the measurement collection script. These parameters will
   * be set by the Measurement Collector based on the current Measurement List, and can be used by
   * the measurement collection script methods.
   *
   * @param parameters list of string parameters supplied by the measurement collection script
   */
  public void setParameters(List<String> parameters) throws Exception {}

  /** The class generic parameter specifies the type of a single raw data point. */
  protected List<T> dataPoints;

  /**
   * Download the raw data points generated during this test run. Then process them to calculate
   * reporting statistics of interest.
   */
  public void processDataPoints(long startTimeMS, long endTimeMS) throws Exception {
    throw new UnsupportedOperationException("downloadDataPoints must be overridden by sub-classes");
  }

  /** Getter for the summary statistics nested object. */
  public MeasurementResultSummary getSummaryStatistics() {
    return summary;
  }

  /** Write the raw data points generated during this test run to a String. */
  public void writeRawDataPointsToFile(File outputFile) throws Exception {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();

    // write the data points as a string
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, dataPoints);
  }
}
