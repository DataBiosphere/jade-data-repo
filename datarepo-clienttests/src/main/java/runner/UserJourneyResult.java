package runner;

public class UserJourneyResult {
  public String userJourneyDescription;
  public String threadName;

  public boolean completed;
  public long elapsedTime;
  public Exception exceptionThrown;

  public UserJourneyResult(String userJourneyDescription, String threadName) {
    this.userJourneyDescription = userJourneyDescription;
    this.threadName = threadName;

    this.exceptionThrown = null;
    this.completed = false;
  }

  public void display() {
    System.out.println("User Journey Result: " + userJourneyDescription);
    System.out.println("  threadName: " + threadName);
    System.out.println("  completed: " + completed);
    System.out.println("  elapsedTime (sec): " + (double) elapsedTime / (1e9));
    System.out.println(
        "  exceptionThrown: " + (exceptionThrown == null ? "" : exceptionThrown.getMessage()));
  }
}
