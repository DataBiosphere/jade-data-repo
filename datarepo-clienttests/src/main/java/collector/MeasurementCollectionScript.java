package collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

public abstract class MeasurementCollectionScript<T> {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementCollectionScript.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public MeasurementCollectionScript() {}

  protected ServerSpecification server;

  /**
   * Setter for the server specification property of this class. This property will be set by the
   * Test Runner based on the current Measurement List, and can be accessed by the measurement
   * collection script methods.
   *
   * @param server the specification of the server(s) where the measurements were collected
   */
  public void setServer(ServerSpecification server) {
    this.server = server;
  }

  /**
   * Setter for any parameters required by the measurement collection script. These parameters will
   * be set by the Test Runner based on the current Measurement List, and can be used by the
   * measurement collection script methods.
   *
   * @param parameters list of string parameters supplied by the test configuration
   */
  public void setParameters(List<String> parameters) throws Exception {}

  /**
   * Setter for the description property of this class. This property will be set by the Test Runner
   * based on the current Measurement List, and can be accessed by the measurement collection script
   * methods.
   *
   * @param description description of measurement
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /** The class generic parameter specifies the type of a single raw data point. */
  protected List<T> dataPoints;

  /** Download the raw data points generated during this test run. */
  public void downloadDataPoints(long startTimeMS, long endTimeMS) throws Exception {
    throw new UnsupportedOperationException("downloadDataPoints must be overridden by sub-classes");
  }

  protected String description;
  protected MeasurementResultSummary summary;

  @SuppressFBWarnings(
      value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
      justification = "This POJO class is used for easy serialization to JSON using Jackson.")
  public static class MeasurementResultSummary {
    public String description;

    public double min;
    public double max;
    public double mean;
    public double standardDeviation;
    public double median;
    public double percentile95;
    public double percentile99;
    public double sum;

    public MeasurementResultSummary(String description) {
      this.description = description;
    }
  }

  /** Process the data points calculating reporting statistics of interest. */
  public void calculateSummaryStatistics() {
    throw new UnsupportedOperationException(
        "calculateSummaryStatistics must be overridden by sub-classes");
  }

  /** Utility method to call standard statistics calculation methods on a set of data. */
  protected void calculateStandardStatistics(DescriptiveStatistics descriptiveStatistics) {
    summary = new MeasurementResultSummary(description);
    summary.max = descriptiveStatistics.getMax();
    summary.min = descriptiveStatistics.getMin();
    summary.mean = descriptiveStatistics.getMean();
    summary.standardDeviation = descriptiveStatistics.getStandardDeviation();
    summary.median = descriptiveStatistics.getPercentile(50);
    summary.percentile95 = descriptiveStatistics.getPercentile(95);
    summary.percentile99 = descriptiveStatistics.getPercentile(99);
    summary.sum = descriptiveStatistics.getSum();
  }

  /** Getter for the summary statistics nested object. */
  public MeasurementResultSummary getSummaryStatistics() {
    return summary;
  }

  /** Write the raw data points generated during this test run to a String. */
  public void writeDataPointsToFile(File outputFile) throws Exception {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();

    // write the data points as a string
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, dataPoints);
  }
}
