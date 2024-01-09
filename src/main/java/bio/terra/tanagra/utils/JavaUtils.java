package bio.terra.tanagra.utils;

import bio.terra.tanagra.exception.SystemException;
import java.time.Duration;

public final class JavaUtils {

  private JavaUtils() {}

  /**
   * Retries until booleanFunction returns true. If booleanFunction never returns true, throws an
   * exception.
   */
  public static void retryUntilTrue(
      int numRetries,
      Duration sleepDuration,
      String errorMessage,
      BooleanFunction booleanFunction) {
    try {
      for (int i = 0; i < numRetries; i++) {
        if (booleanFunction.run()) {
          return;
        }
        Thread.sleep(sleepDuration.toMillis());
      }
      throw new SystemException(errorMessage);
    } catch (InterruptedException e) {
      throw new SystemException("runWithRetriesUntilTrue() was interrupted", e);
    }
  }
}
