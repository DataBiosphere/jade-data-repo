package runner;

public class UserJourneyResult {
  public String testScriptName;
  public String threadName;

  public Exception exceptionThrown;
  public boolean completed;

  public UserJourneyResult(String testScriptName, String threadName) {
    this.testScriptName = testScriptName;
    this.threadName = threadName;

    this.exceptionThrown = null;
    this.completed = false;
  }

  public void display() {
    System.out.println("User Journey Result: " + testScriptName);
    System.out.println("  threadName: " + threadName);
    System.out.println("  completed: " + completed);
    System.out.println(
        "  exceptionThrown: " + (exceptionThrown == null ? "" : exceptionThrown.getMessage()));
  }
}
