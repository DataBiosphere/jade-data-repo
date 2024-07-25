package bio.terra.service.common;

import bio.terra.common.BoundedAsyncTaskExecutor;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * A service to instrument any {@link BoundedAsyncTaskExecutor}s registered as Spring Beans in
 * Micrometer.
 *
 * <p>While {@link org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor}s are
 * automatically instrumented by Micrometer via Actuator, our wrapper throttles submissions to these
 * executors rather than rejecting them at saturation using a {@link Semaphore}.
 *
 * <p>We want visibility into:
 *
 * <ul>
 *   <li>The Semaphore's available permits
 *   <li>The Semaphore's "queue length" (estimated number of threads waiting to acquire a permit)
 * </ul>
 */
@Component
public class BoundedAsyncTaskExecutorMetricsService {

  private static final Logger logger =
      LoggerFactory.getLogger(BoundedAsyncTaskExecutorMetricsService.class);
  static final String PREFIX = "datarepo.boundedAsyncTaskExecutor.";
  /**
   * A String template for the name of an executor's available permits gauge, expecting a Bean name.
   */
  static final String AVAILABLE_PERMITS_TEMPLATE = PREFIX + "%s.availablePermits";
  /** A String template for the name of an executor's queue length gauge, expecting a Bean name. */
  static final String QUEUE_LENGTH_TEMPLATE = PREFIX + "%s.queueLength";

  private final ApplicationContext applicationContext;
  private final MeterRegistry meterRegistry;

  public BoundedAsyncTaskExecutorMetricsService(
      ApplicationContext applicationContext, MeterRegistry meterRegistry) {
    this.applicationContext = applicationContext;
    this.meterRegistry = meterRegistry;
  }

  @PostConstruct
  @VisibleForTesting
  void registerGauges() {
    applicationContext.getBeansOfType(BoundedAsyncTaskExecutor.class).forEach(this::registerGauges);
  }

  private void registerGauges(String beanName, BoundedAsyncTaskExecutor boundedAsyncTaskExecutor) {
    logger.info("Registering Micrometer gauges on {} (available permits, queue length)", beanName);
    Semaphore semaphore = boundedAsyncTaskExecutor.getSemaphore();

    var availablePermitsName = AVAILABLE_PERMITS_TEMPLATE.formatted(beanName);
    meterRegistry.gauge(availablePermitsName, semaphore, Semaphore::availablePermits);

    var queueLengthName = QUEUE_LENGTH_TEMPLATE.formatted(beanName);
    meterRegistry.gauge(queueLengthName, semaphore, Semaphore::getQueueLength);
  }
}
