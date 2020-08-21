package runner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
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

    public long minElapsedTimeMS;
    public long maxElapsedTimeMS;
    public double meanElapsedTimeMS;

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
    for (int ctr = 0; ctr < userJourneyResults.size(); ctr++) {
      UserJourneyResult result = userJourneyResults.get(ctr);

      // calculate the min, max, mean elapsed time
      if (summary.minElapsedTimeMS > result.elapsedTimeNS || ctr == 0) {
        summary.minElapsedTimeMS = result.elapsedTimeNS;
      }
      if (summary.maxElapsedTimeMS < result.elapsedTimeNS || ctr == 0) {
        summary.maxElapsedTimeMS = result.elapsedTimeNS;
      }
      summary.meanElapsedTimeMS += result.elapsedTimeNS;

      // count the number of user journeys that completed and threw exceptions
      summary.numCompleted += (result.completed) ? 1 : 0;
      summary.numExceptionsThrown += (result.exceptionThrown != null) ? 1 : 0;
    }
    summary.meanElapsedTimeMS /= userJourneyResults.size();
    summary.totalRun = userJourneyResults.size();

    summary.isFailure =
        (summary.numCompleted < summary.totalRun) || (summary.numExceptionsThrown > 0);

    // convert all the times from nanoseconds to milliseconds
    summary.minElapsedTimeMS /= (1e6);
    summary.maxElapsedTimeMS /= (1e6);
    summary.meanElapsedTimeMS /= (1e6);
  }
}
