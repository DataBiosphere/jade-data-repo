package bio.terra.service.filedata;

import bio.terra.app.controller.exception.TooManyRequestsException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class DrsMetricsService {
  private static final String NAME_PREFIX = "datarepo.drs";
  private static final String REQUEST_COUNT_NAME_PREFIX = NAME_PREFIX + ".requestCount";
  static final String REQUEST_COUNT_GAUGE_NAME = REQUEST_COUNT_NAME_PREFIX + ".gauge";
  static final String REQUEST_COUNT_MAX_GAUGE_NAME = REQUEST_COUNT_NAME_PREFIX + ".max";

  private final AtomicInteger currentDrsRequestCount;
  private final AtomicInteger drsRequestCountMax;

  public DrsMetricsService(MeterRegistry meterRegistry) {
    this.currentDrsRequestCount =
        meterRegistry.gauge(REQUEST_COUNT_GAUGE_NAME, new AtomicInteger(0));
    this.drsRequestCountMax =
        meterRegistry.gauge(REQUEST_COUNT_MAX_GAUGE_NAME, new AtomicInteger(0));
  }

  /**
   * Increment request count gauge representing the DRS requests currently being serviced, meant to
   * be called on request arrival.
   *
   * @throws TooManyRequestsException if incrementing the request count would exceed the maximum
   *     allowed
   */
  public void tryIncrementCurrentDrsRequestCount() throws TooManyRequestsException {
    if (currentDrsRequestCount.get() >= drsRequestCountMax.get()) {
      throw new TooManyRequestsException(
          "Too many DataRepositoryService requests are being made at once. Please try again later.");
    }
    currentDrsRequestCount.incrementAndGet();
  }

  /**
   * Decrement request count gauge representing the DRS requests currently being serviced, meant to
   * be called on request response.
   */
  public void decrementCurrentDrsRequestCount() {
    currentDrsRequestCount.decrementAndGet();
  }

  public void setDrsRequestMax(int newMax) {
    drsRequestCountMax.set(newMax);
  }
}
