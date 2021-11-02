package bio.terra.common;

import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.UpdatePermissionsFailedException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.bigquery.BigQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclUtils {
  private static final Logger logger = LoggerFactory.getLogger(AclUtils.class);

  private static final int RETRIES = 15;
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int INITIAL_WAIT_SECONDS = 2;

  public static <T> T aclUpdateRetry(Callable<T> aclUpdate) throws InterruptedException {
    Random random = new Random();
    Throwable lastException = null;
    int retryWait = INITIAL_WAIT_SECONDS;
    for (int i = 0; i < RETRIES; i++) {
      try {
        return aclUpdate.call();
      } catch (Exception ex) {
        if (FireStoreUtils.shouldRetry(ex.getCause(), false)) {
          logger.info(
              String.format(
                  "Failed to update ACL due to [%s]. Retry %d of %d", ex.getCause(), i, RETRIES),
              ex);
          lastException = ex.getCause();
        } else {
          throw new GoogleResourceException("Error while performing ACL update", ex);
        }

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
}
