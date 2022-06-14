package bio.terra.common;

import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.UpdatePermissionsFailedException;
import java.security.SecureRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclUtils {
  private static final Logger logger = LoggerFactory.getLogger(AclUtils.class);

  private static final int RETRIES = 15;
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int INITIAL_WAIT_SECONDS = 2;

  public static <T> T aclUpdateRetry(Callable<T> aclUpdate) throws InterruptedException {
    SecureRandom random = new SecureRandom();
    Throwable lastException = null;
    int retryWait = INITIAL_WAIT_SECONDS;
    for (int i = 0; i < RETRIES; i++) {
      try {
        return aclUpdate.call();
      } catch (AclRetryException ex) {
        logger.info(
            String.format(
                "Failed to update ACL due to [%s]. Retry %d of %d", ex.getReason(), i, RETRIES),
            ex);
        lastException = ex.getCause();
      } catch (Exception ex) {
        throw new GoogleResourceException("Error while performing ACL update", ex);
      }

      TimeUnit.SECONDS.sleep(retryWait);
      retryWait = retryWait + retryWait;
      if (retryWait > MAX_WAIT_SECONDS) {
        // Make it fuzzy to decrease chance of a bunch of stuff executing together
        retryWait = MAX_WAIT_SECONDS + random.nextInt(10);
      }
    }
    throw new UpdatePermissionsFailedException("Cannot update ACL permissions", lastException);
  }

  public static class AclRetryException extends RuntimeException {

    private final String reason;

    public AclRetryException(String message, Exception cause, String reason) {
      super(message, cause);
      this.reason = reason;
    }

    public String getReason() {
      return reason;
    }
  }
}
