package runner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
    value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class UserJourneyResult {
  public String userJourneyDescription;

  public String threadName;

  public boolean completed;
  public long elapsedTimeNS;
  public Exception exceptionThrown;

  public UserJourneyResult(String userJourneyDescription, String threadName) {
    this.userJourneyDescription = userJourneyDescription;
    this.threadName = threadName;

    this.exceptionThrown = null;
    this.completed = false;
  }
}
