package runner;

import common.BasicStatistics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import runner.config.TestScriptSpecification;

public class TestScriptResult {
  public List<UserJourneyResult> userJourneyResults;
  public TestScriptResultSummary summary;

  /**
   * Summary statistics are pulled out into a separate inner class for easier summary reporting.
   * This class does not include a reference to the full TestScriptSpecification or the list of
   * UserJourneyResults.
   */
  @SuppressFBWarnings(
      value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
      justification = "This POJO class is used for easy serialization to JSON using Jackson.")
  public static class TestScriptResultSummary {
    public String testScriptDescription;

    public BasicStatistics elapsedTimeStatistics;

    public int totalRun; // total number of user journey threads submitted to the thread pool
    public int numCompleted; // number of user journey threads that completed
    public int numExceptionsThrown; // number of user journey threads that threw exceptions

    public boolean isFailure; // numCompleted < totalRun

    public TestScriptResultSummary() {} // default constructor so Jackson can deserialize

    private TestScriptResultSummary(String testScriptDescription) {
      this.testScriptDescription = testScriptDescription;
    }
  }

  public TestScriptResult(
      TestScriptSpecification testScriptSpecification, List<UserJourneyResult> userJourneyResults) {
    this.userJourneyResults = userJourneyResults;

    summary = new TestScriptResultSummary(testScriptSpecification.description);
    calculateStatistics();
  }

  public TestScriptResultSummary getSummary() {
    return summary;
  }

  /** Loop through the UserJourneyResults calculating reporting statistics of interest. */
  private void calculateStatistics() {
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (int ctr = 0; ctr < userJourneyResults.size(); ctr++) {
      UserJourneyResult result = userJourneyResults.get(ctr);

      // count the number of user journeys that completed and threw exceptions
      summary.numCompleted += (result.completed) ? 1 : 0;
      summary.numExceptionsThrown += (result.exceptionThrown != null) ? 1 : 0;

      // convert elapsed time from nanosecods to milliseconds
      descriptiveStatistics.addValue(result.elapsedTimeNS / (1e6));
    }
    summary.elapsedTimeStatistics =
        BasicStatistics.calculateStandardStatistics(descriptiveStatistics);
    summary.totalRun = userJourneyResults.size();

    summary.isFailure =
        (summary.numCompleted < summary.totalRun) || (summary.numExceptionsThrown > 0);
  }
}
