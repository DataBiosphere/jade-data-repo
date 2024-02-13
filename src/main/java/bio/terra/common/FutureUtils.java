package bio.terra.common;

import bio.terra.common.exception.ApiException;
import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.exception.ServiceUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public final class FutureUtils {

  private FutureUtils() {}

  /**
   * Wait indefinitely for a list of {@link Future} objects to complete and returns a list of the
   * resolved values. If any of the futures fail, throws the first found instance of failure
   * (non-deterministic) and cancel any active futures remaining.
   *
   * @param futures The {@link List} of futures to wait for
   * @param <T> The type that should be returned for the list of futures
   * @return The resolved values of the futures with nulls removed. There is no guarantee that the
   *     order of returned values matches the order of the passed in futures
   */
  public static <T> List<@NotNull T> waitFor(final List<Future<T>> futures) {
    return waitFor(futures, null);
  }

  /**
   * Wait for a list of {@link Future} objects to complete and returns a list of the resolved values
   * with nulls removed. If any of the futures fail, throws the first found instance of failure
   * (non-deterministic) and cancel any active futures remaining.
   *
   * @param futures The {@link List} of futures to wait for
   * @param maxThreadWait The maximum of time to wait per future
   * @param <T> The type that should be returned for the list of futures
   * @return The resolved values of the futures with nulls removed. There is no guarantee that the
   *     order of returned values matches the order of the passed in futures
   */
  @VisibleForTesting
  static <T> List<@NotNull T> waitFor(final List<Future<T>> futures, final Duration maxThreadWait) {
    final AtomicReference<ErrorReportException> foundFailure = new AtomicReference<>();
    List<T> returnList =
        futures.stream()
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
                    // Cancellation may not be necessary, but it can't hurt:
                    f.cancel(true);
                  } catch (InterruptedException e) {
                    foundFailure.compareAndSet(null, new ApiException("Thread was interrupted", e));
                    // Cancellation may not be necessary, but it can't hurt:
                    f.cancel(true);
                  } catch (ExecutionException e) {
                    if (e.getCause() instanceof ErrorReportException ere) {
                      // We do not wrap an ErrorReportException cause to preserve its HTTP status.
                      foundFailure.compareAndSet(null, ere);
                    } else {
                      foundFailure.compareAndSet(
                          null, new ApiException("Error executing thread", e));
                    }
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .toList();

    // Throw an exception if any of the tasks failed
    ErrorReportException maybeException = foundFailure.get();
    if (maybeException != null) {
      throw maybeException;
    }
    return returnList;
  }
}
