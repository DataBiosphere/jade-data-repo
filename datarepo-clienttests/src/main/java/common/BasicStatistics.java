package common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

@SuppressFBWarnings(
    value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class BasicStatistics {

  public double min;
  public double max;
  public double mean;
  public double standardDeviation;
  public double median;
  public double percentile95;
  public double percentile99;
  public double sum;

  public BasicStatistics() {}

  /** Utility method to call standard statistics calculation methods on a set of data. */
  public static BasicStatistics calculateStandardStatistics(
      DescriptiveStatistics descriptiveStatistics) {
    BasicStatistics stats = new BasicStatistics();

    stats.max = descriptiveStatistics.getMax();
    stats.min = descriptiveStatistics.getMin();
    stats.mean = descriptiveStatistics.getMean();
    stats.standardDeviation = descriptiveStatistics.getStandardDeviation();
    stats.median = descriptiveStatistics.getPercentile(50);
    stats.percentile95 = descriptiveStatistics.getPercentile(95);
    stats.percentile99 = descriptiveStatistics.getPercentile(99);
    stats.sum = descriptiveStatistics.getSum();

    return stats;
  }
}
