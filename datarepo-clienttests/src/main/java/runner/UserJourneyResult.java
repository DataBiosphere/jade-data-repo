package runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserJourneyResult {
  private static final Logger logger = LoggerFactory.getLogger(UserJourneyResult.class);

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
