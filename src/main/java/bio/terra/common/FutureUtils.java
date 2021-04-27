package bio.terra.common;

import bio.terra.app.controller.exception.ApiException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum FutureUtils {
    _UNUSED;

    /**
     * Wait for a list of {@link Future} objects to complete and returns a list of the resolved values.
     * If any of the threads fail, throws the first found instance of failure (non-deterministic) but waits for all
     * futures to resolve.
     * @param futures The {@link List} of futures to wait for
     * @param <T> The type that should be returned for the list of futures
     * @return The resolved values of the futures.  There is no guarantee that the order of returned values matches the
     * order of the passed in futures
     */
    public static <T> List<T> waitFor(final List<Future<T>> futures) {
        final AtomicReference<ApiException> foundFailure = new AtomicReference<>();
        try (Stream<Future<T>> stream = futures.stream()) {
            List<T> returnList = stream
                .map(f -> {
                    try {
                        if (foundFailure.get() == null) {
                            return f.get();
                        }
                        // If a failure was found, all subsequent tasks should be canceled
                        f.cancel(true);
                    } catch (InterruptedException e) {
                        foundFailure.compareAndSet(null, new ApiException("Thread was interrupted", e));
                    } catch (ExecutionException e) {
                        foundFailure.compareAndSet(null, new ApiException("Error executing thread", e));
                    }
                    // The return value here is ignored because this will ultimately result in throwing an exception,
                    // since foundFailure must be non-null if this line is reached.
                    return null;
                })
                .collect(Collectors.toList());

            // Throw an exception if any of the tasks failed
            final ApiException e = foundFailure.get();
            if (e != null) {
                throw e;
            }
            return returnList;
        }
    }
}
