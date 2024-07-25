package bio.terra.common;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * An implementation of {@link RejectedExecutionHandler}.
 *
 * <p>If a {@link ThreadPoolExecutor} can't accept a task due to saturation of its threadpool and
 * queue, this handler blocks when waiting for a spot to become available in the queue rather than
 * rejecting the task.
 */
public class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {
  @VisibleForTesting
  static String interruptedExceptionMessage(Runnable r, ThreadPoolExecutor executor) {
    return "Task %s interrupted while waiting to be added to queue: %s".formatted(r, executor);
  }

  @VisibleForTesting
  static String executorShutdownMessage(Runnable r, ThreadPoolExecutor executor) {
    return "%s is shutting down and cannot accept task %s".formatted(executor, r);
  }

  /**
   * Method that may be invoked by a {@link ThreadPoolExecutor} when {@link
   * ThreadPoolExecutor#execute execute} cannot accept a task.
   *
   * <p>If the reason for invocation is that the Executor is saturated (no more threads or queue
   * slots available), then the runnable will be added to the queue, blocking until space is
   * available.
   *
   * <p>If the Executor has been shut down or the thread is interrupted, the method will throw a
   * {@link RejectedExecutionException}, which will be propagated to the caller of {@code execute}.
   *
   * @param r the runnable task requested to be executed
   * @param executor the executor attempting to execute this task
   * @throws RejectedExecutionException if there is no remedy
   */
  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    if (!executor.isShutdown()) {
      try {
        // Block until space becomes available in the queue
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException(interruptedExceptionMessage(r, executor), e);
      }
    } else {
      throw new RejectedExecutionException(executorShutdownMessage(r, executor));
    }
  }
}
