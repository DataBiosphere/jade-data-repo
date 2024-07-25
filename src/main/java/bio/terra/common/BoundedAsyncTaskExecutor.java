package bio.terra.common;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

public class BoundedAsyncTaskExecutor {

  private static final Logger logger = LoggerFactory.getLogger(BoundedAsyncTaskExecutor.class);
  private final AsyncTaskExecutor executor;
  private final Semaphore semaphore;

  /**
   * A wrapper for an executor which throttles submissions under heavy load, rather than rejecting
   * tasks.
   *
   * <p>Any instances registered as Spring Beans will have their underlying Semaphore instrumented
   * in Micrometer via {@link bio.terra.service.common.BoundedAsyncTaskExecutorMetricsService}.
   *
   * <p>Based on <a href="https://jcip.net/listings/BoundedExecutor.java">"Java Concurrency in
   * Practice"</a>.
   */
  public BoundedAsyncTaskExecutor(AsyncTaskExecutor executor, int permits) {
    this.executor = executor;
    this.semaphore = new Semaphore(permits);
    logger.info("Created BoundedAsyncTaskExecutor with {} available permits", permits);
  }

  public AsyncTaskExecutor getExecutor() {
    return this.executor;
  }

  public Semaphore getSemaphore() {
    return this.semaphore;
  }

  /**
   * Submit a Callable task for execution, receiving a Future representing that task. The Future
   * will return the Callable's result upon completion.
   *
   * <p>If the executor's threads and queue are all occupied, tasks will not be rejected and this
   * method will block until a permit becomes available in this object's {@link Semaphore}.
   *
   * @param task the {@code Callable} to execute (never {@code null})
   * @return a Future representing pending completion of the task
   * @throws InterruptedException if the thread was interrupted
   */
  public <T> Future<T> submit(Callable<T> task) throws InterruptedException {
    logger.info("Permits before submission: " + semaphore.availablePermits());
    semaphore.acquire();
    logger.info("Permits when Semaphore acquired: " + semaphore.availablePermits());
    try {
      return executor.submit(
          () -> {
            try {
              return task.call();
            } finally {
              semaphore.release();
              logger.info("Permits after Semaphore released: " + semaphore.availablePermits());
            }
          });
    } catch (RejectedExecutionException ex) {
      // Execution should not be rejected when the executor and its queue are fully saturated
      // (that's the point of the Semaphore), but if execution is rejected for some other reason,
      // let's release the permit we took out.
      semaphore.release();
      throw ex;
    }
  }

  /**
   * Submit a Runnable task for execution, receiving a Future representing that task. The Future
   * will return a {@code null} result upon completion.
   *
   * <p>If the executor's threads and queue are all occupied, tasks will not be rejected and this
   * method will block until a permit becomes available in this object's {@link Semaphore}.
   *
   * @param task the {@code Runnable} to execute (never {@code null})
   * @return a Future representing pending completion of the task
   * @throws InterruptedException if the thread was interrupted
   */
  public Future<?> submit(Runnable task) throws InterruptedException {
    logger.info("Permits before submission: " + semaphore.availablePermits());
    semaphore.acquire();
    logger.info("Permits after Semaphore acquired: " + semaphore.availablePermits());
    try {
      return executor.submit(
          () -> {
            try {
              task.run();
            } finally {
              semaphore.release();
              logger.info("Permits after Semaphore released: " + semaphore.availablePermits());
            }
          });
    } catch (RejectedExecutionException ex) {
      // Execution should not be rejected when the executor and its queue are fully saturated
      // (that's the point of the Semaphore), but if execution is rejected for some other reason,
      // let's release the permit we took out.
      semaphore.release();
      throw ex;
    }
  }
}
