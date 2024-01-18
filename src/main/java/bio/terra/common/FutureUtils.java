package bio.terra.common;

import bio.terra.common.exception.ApiException;
import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.exception.ServiceUnavailableException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class FutureUtils {

  private FutureUtils() {}

  public static <T> List<T> waitFor(final List<Future<T>> futures) {
    return waitFor(futures, null);
  }

  /**
   * Wait for a list of {@link Future} objects to complete and returns a list of the resolved
   * values. If any of the threads fail, throws the first found instance of failure
   * (non-deterministic) but waits for all futures to resolve.
   *
   * @param futures The {@link List} of futures to wait for
   * @param maxThreadWait The maximum of time to wait per thread
   * @param <T> The type that should be returned for the list of futures
   * @return The resolved values of the futures. There is no guarantee that the order of returned
   *     values matches the order of the passed in futures
   */
  public static <T> List<T> waitFor(final List<Future<T>> futures, final Duration maxThreadWait) {

    final AtomicReference<ErrorReportException> foundFailure = new AtomicReference<>();
    try (Stream<Future<T>> stream = futures.stream()) {
      List<T> returnList =
          stream
              .map(
                  f -> {
                    try {
                      // If a failure was found, all subsequent tasks should be canceled
                      if (foundFailure.get() != null) {
                        f.cancel(true);
                      } else if (maxThreadWait != null) {
                        return f.get(maxThreadWait.toMillis(), TimeUnit.MILLISECONDS);
                      } else {
                        return f.get();
                      }
                    } catch (TimeoutException e) {
                      foundFailure.compareAndSet(
                          null, new ServiceUnavailableException("Thread timed out", e));
                      f.cancel(true); // Do we need this?
                    } catch (InterruptedException e) {
                      foundFailure.compareAndSet(
                          null, new ApiException("Thread was interrupted", e));
                      f.cancel(true); // Do we need this?
                    } catch (ExecutionException e) {
                      if (e.getCause() instanceof ErrorReportException ere) {
                        // We do not wrap an ErrorReportException cause to preserve its HTTP status.
                        foundFailure.compareAndSet(null, ere);
                      } else {
                        foundFailure.compareAndSet(
                            null, new ApiException("Error executing thread", e));
                      }
                    }
                    // Returning null here but this will ultimately result in throwing an exception
                    // to results should
                    // never be read
                    return null;
                  })
              .toList();

      // Throw an exception if any of the tasks failed
      ErrorReportException maybeException = foundFailure.get();
      if (maybeException != null) {
        throw maybeException;
      }
      return returnList;
    }
  }
}
