package bio.terra.tanagra.utils;

import bio.terra.tanagra.exception.SystemException;
import java.time.Duration;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for making HTTP requests. */
public class HttpUtils {
  // default value for the maximum number of times to retry HTTP requests
  public static final int DEFAULT_MAXIMUM_RETRIES = 15;
  // default value for the time to sleep between retries
  public static final Duration DEFAULT_DURATION_SLEEP_FOR_RETRY = Duration.ofSeconds(1);
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

  private HttpUtils() {}

  /**
   * Helper method to call a function with retries. Uses {@link #DEFAULT_MAXIMUM_RETRIES} for
   * maximum number of retries and {@link #DEFAULT_DURATION_SLEEP_FOR_RETRY} for the time to sleep
   * between retries.
   *
   * @param makeRequest function to perform the request
   * @param isRetryable function to test whether the exception is retryable or not
   * @throws E if makeRequest throws an exception that is not retryable
   * @throws SystemException if the maximum number of retries is exhausted, and the last attempt
   *     threw a retryable exception
   */
  public static <E extends Exception> void callWithRetries(
      RunnableWithCheckedException<E> makeRequest, Predicate<Exception> isRetryable)
      throws E, InterruptedException {
    callWithRetries(
        () -> {
          makeRequest.run();
          return null;
        },
        isRetryable,
        DEFAULT_MAXIMUM_RETRIES,
        DEFAULT_DURATION_SLEEP_FOR_RETRY);
  }

  /**
   * Helper method to call a function with retries. Uses {@link #DEFAULT_MAXIMUM_RETRIES} for
   * maximum number of retries and {@link #DEFAULT_DURATION_SLEEP_FOR_RETRY} for the time to sleep
   * between retries.
   *
   * @param makeRequest function to perform the request
   * @param isRetryable function to test whether the exception is retryable or not
   * @param <T> type of the response object (i.e. return type of the makeRequest function)
   * @return the response object
   * @throws E if makeRequest throws an exception that is not retryable
   * @throws SystemException if the maximum number of retries is exhausted, and the last attempt
   *     threw a retryable exception
   */
  public static <T, E extends Exception> T callWithRetries(
      SupplierWithCheckedException<T, E> makeRequest, Predicate<Exception> isRetryable)
      throws E, InterruptedException {
    return callWithRetries(
        makeRequest, isRetryable, DEFAULT_MAXIMUM_RETRIES, DEFAULT_DURATION_SLEEP_FOR_RETRY);
  }

  /**
   * Helper method to call a function with retries.
   *
   * @param <T> type of the response object (i.e. return type of the makeRequest function)
   * @param makeRequest function to perform the request
   * @param isRetryable function to test whether the exception is retryable or not
   * @param maxCalls maximum number of times to retry
   * @param sleepDuration time to sleep between tries
   * @return the response object
   * @throws E if makeRequest throws an exception that is not retryable
   * @throws SystemException if the maximum number of retries is exhausted, and the last attempt
   *     threw a retryable exception
   */
  public static <T, E extends Exception> T callWithRetries(
      SupplierWithCheckedException<T, E> makeRequest,
      Predicate<Exception> isRetryable,
      int maxCalls,
      Duration sleepDuration)
      throws E, InterruptedException {
    // isDone always return true
    return pollWithRetries(makeRequest, result -> true, isRetryable, maxCalls, sleepDuration);
  }

  /**
   * Helper method to poll with retries. Uses {@link #DEFAULT_MAXIMUM_RETRIES} for maximum number of
   * retries and {@link #DEFAULT_DURATION_SLEEP_FOR_RETRY} for the time to sleep between retries.
   *
   * @param makeRequest function to perform the request
   * @param isRetryable function to test whether the exception is retryable or not
   * @param <T> type of the response object (i.e. return type of the makeRequest function)
   * @return the response object
   * @throws E if makeRequest throws an exception that is not retryable
   * @throws SystemException if the maximum number of retries is exhausted, and the last attempt
   *     threw a retryable exception
   */
  public static <T, E extends Exception> T pollWithRetries(
      SupplierWithCheckedException<T, E> makeRequest,
      Predicate<T> isDone,
      Predicate<Exception> isRetryable)
      throws E, InterruptedException {
    return pollWithRetries(
        makeRequest,
        isDone,
        isRetryable,
        DEFAULT_MAXIMUM_RETRIES,
        DEFAULT_DURATION_SLEEP_FOR_RETRY);
  }

  /**
   * Helper method to poll with retries.
   *
   * <p>If there is no timeout, the method returns the last result.
   *
   * <p>If there is a timeout, the behavior depends on the last attempt.
   *
   * <p>- If the last attempt produced a result that is not done (i.e. isDone returns false), then
   * the result is returned.
   *
   * <p>- If the last attempt threw a retryable exception, then this method re-throws that last
   * exception wrapped in a {@link SystemException} with a timeout message.
   *
   * @param <T> type of the response object (i.e. return type of the makeRequest function)
   * @param makeRequest function to perform the request
   * @param isDone function to decide whether to keep polling or not, based on the result
   * @param isRetryable function to test whether the exception is retryable or not
   * @param maxCalls maximum number of times to poll or retry
   * @param sleepDuration time to sleep between tries
   * @return the response object
   * @throws E if makeRequest throws an exception that is not retryable
   * @throws SystemException if the maximum number of retries is exhausted, and the last attempt
   *     threw a retryable exception
   */
  public static <T, E extends Exception> T pollWithRetries(
      SupplierWithCheckedException<T, E> makeRequest,
      Predicate<T> isDone,
      Predicate<Exception> isRetryable,
      int maxCalls,
      Duration sleepDuration)
      throws E, InterruptedException {
    int numTries = 0;
    Exception lastRetryableException = null;
    do {
      numTries++;
      try {
        LOGGER.debug("Request attempt #{}/{}", numTries - 1, maxCalls);

        T result = makeRequest.makeRequest();
        LOGGER.debug("Result: {}", result);

        boolean jobCompleted = isDone.test(result);
        boolean timedOut = numTries > maxCalls;
        if (jobCompleted || timedOut) {
          // polling is either done (i.e. job completed) or timed out: return the last result
          LOGGER.debug(
              "polling with retries completed. jobCompleted = {}, timedOut = {}",
              jobCompleted,
              timedOut);
          return result;
        }
      } catch (Exception ex) {
        if (!isRetryable.test(ex)) {
          // the exception is not retryable: re-throw
          throw ex;
        } else {
          // keep track of the last retryable exception so we can re-throw it in case of a timeout
          lastRetryableException = ex;
        }
        LOGGER.debug("Caught retryable exception", ex);
      }

      // sleep before retrying, unless this is the last try
      if (numTries < maxCalls) {
        Thread.sleep(sleepDuration.toMillis());
      }
    } while (numTries <= maxCalls);

    // request with retries timed out: re-throw the last exception
    throw new SystemException(
        "Request with retries timed out after " + numTries + " tries.", lastRetryableException);
  }

  /**
   * Function interface for making a retryable Http request. This interface is explicitly defined so
   * that it can throw an exception (i.e. Supplier does not have this method annotation).
   *
   * @param <T> type of the Http response (i.e. return type of the makeRequest method)
   */
  @FunctionalInterface
  public interface SupplierWithCheckedException<T, E extends Exception> {
    T makeRequest() throws E, InterruptedException;
  }

  /**
   * Function interface for making a retryable Http request. This interface is explicitly defined so
   * that it can throw an exception (i.e. Runnable does not have this method annotation).
   */
  @FunctionalInterface
  public interface RunnableWithCheckedException<E extends Exception> {
    void run() throws E, InterruptedException;
  }
}
