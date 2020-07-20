package runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserJourneyResult {
  private static final Logger LOG = LoggerFactory.getLogger(UserJourneyResult.class);

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
    LOG.info("User Journey Result: {}", userJourneyDescription);
    LOG.info("  threadName: {}", threadName);
    LOG.info("  completed: {}", completed);
    LOG.info("  elapsedTime (sec): {}", elapsedTime / (1e9));
    LOG.info("  exceptionThrown: {}", exceptionThrown == null, exceptionThrown);
  }
}
