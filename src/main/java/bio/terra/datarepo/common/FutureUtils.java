package bio.terra.common;

import bio.terra.app.controller.exception.ApiException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FutureUtils {

  private FutureUtils() {}

  public static <T> List<T> waitFor(final List<Future<T>> futures) {
    return waitFor(futures, Optional.empty());
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
  public static <T> List<T> waitFor(
      final List<Future<T>> futures, final Optional<Duration> maxThreadWait) {

    final AtomicReference<Optional<ApiException>> foundFailure =
        new AtomicReference<>(Optional.empty());
    try (Stream<Future<T>> stream = futures.stream()) {
      List<T> returnList =
          stream
              .map(
                  f -> {
                    try {
                      // If a failure was found, all subsequent tasks should be canceled
                      if (foundFailure.get().isPresent()) {
                        f.cancel(true);
                      } else if (maxThreadWait.isPresent()) {
                        return f.get(maxThreadWait.get().toMillis(), TimeUnit.MILLISECONDS);
                      } else {
                        return f.get();
                      }
                    } catch (TimeoutException e) {
                      if (!foundFailure.get().isPresent()) {
                        foundFailure.set(Optional.of(new ApiException("Thread timed out", e)));
                        // Cancel the thread
                        f.cancel(true);
                      }
                    } catch (InterruptedException e) {
                      if (!foundFailure.get().isPresent()) {
                        foundFailure.set(
                            Optional.of(new ApiException("Thread was interrupted", e)));
                      }
                    } catch (ExecutionException e) {
                      if (!foundFailure.get().isPresent()) {
                        foundFailure.set(
                            Optional.of(new ApiException("Error executing thread", e)));
                      }
                    }
                    // Returning null here but this will ultimately result in throwing an exception
                    // to results should
                    // never be read
                    return null;
                  })
              .collect(Collectors.toList());

      // Throw an exception if any of the tasks failed
      foundFailure
          .get()
          .ifPresent(
              e -> {
                throw e;
              });
      return returnList;
    }
  }
}
