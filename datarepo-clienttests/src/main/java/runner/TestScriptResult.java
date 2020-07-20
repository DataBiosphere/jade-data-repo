package runner;

import java.util.List;
import runner.config.TestScriptSpecification;

public class TestScriptResult {
  public TestScriptSpecification testScriptSpecification;
  public List<UserJourneyResult> userJourneyResults;
  public TestScriptResultSummary summary;

  
  public static class TestScriptResultSummary {
    public String testScriptDescription;

    public long minElapsedTimeMS;
    public long maxElapsedTimeMS;
    public double meanElapsedTimeMS;

    public int totalRun;
    public int numCompleted;
    public int numExceptionsThrown;

    public boolean isFailure;

    private TestScriptResultSummary(String testScriptDescription) {
      this.testScriptDescription = testScriptDescription;
    }
  }

  public TestScriptResult(
      TestScriptSpecification testScriptSpecification, List<UserJourneyResult> userJourneyResults) {
    this.testScriptSpecification = testScriptSpecification;
    this.userJourneyResults = userJourneyResults;

    summary = new TestScriptResultSummary(testScriptSpecification.description);
    calculateStatistics();
  }

  public TestScriptResultSummary getSummary() {
    return summary;
  }

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

    summary.isFailure = summary.numCompleted < summary.totalRun;

    // convert all the times from nanoseconds to milliseconds
    summary.minElapsedTimeMS /= (1e6);
    summary.maxElapsedTimeMS /= (1e6);
    summary.meanElapsedTimeMS /= (1e6);
  }
}
