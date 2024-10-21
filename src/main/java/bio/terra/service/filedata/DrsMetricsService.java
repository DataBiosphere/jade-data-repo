package bio.terra.service.filedata;

import bio.terra.app.controller.exception.TooManyRequestsException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class DrsMetricsService {
  private static final String NAME_PREFIX = "datarepo.drs";
  private static final String OPEN_REQUEST_NAME_PREFIX = NAME_PREFIX + ".openRequests";
  static final String OPEN_REQUEST_GAUGE_NAME = OPEN_REQUEST_NAME_PREFIX + ".gauge";
  static final String OPEN_REQUEST_MAX_GAUGE_NAME = OPEN_REQUEST_NAME_PREFIX + ".max";

  private final AtomicInteger currentDrsRequestCount;
  private final AtomicInteger drsRequestCountMax;

  public DrsMetricsService(MeterRegistry meterRegistry) {
    this.currentDrsRequestCount =
        meterRegistry.gauge(OPEN_REQUEST_GAUGE_NAME, new AtomicInteger(0));
    this.drsRequestCountMax =
        meterRegistry.gauge(OPEN_REQUEST_MAX_GAUGE_NAME, new AtomicInteger(0));
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

  /**
   * @param newMax value to set as the maximum number of concurrent DRS requests allowed per pod.
   */
  public void setDrsRequestMax(int newMax) {
    drsRequestCountMax.set(newMax);
  }
}
