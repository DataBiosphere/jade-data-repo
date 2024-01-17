package bio.terra.service.filedata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.common.category.Unit;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DrsMetricsServiceTest {
  @Mock private MeterRegistry meterRegistry;
  private AtomicInteger currentDrsRequestCount;
  private AtomicInteger drsRequestCountMax;

  private DrsMetricsService drsMetricsService;

  @BeforeEach
  void beforeEach() {
    this.currentDrsRequestCount = new AtomicInteger(0);
    this.drsRequestCountMax = new AtomicInteger(1);

    when(meterRegistry.gauge(eq(DrsMetricsService.OPEN_REQUEST_GAUGE_NAME), any()))
        .thenReturn(currentDrsRequestCount);
    when(meterRegistry.gauge(eq(DrsMetricsService.OPEN_REQUEST_MAX_GAUGE_NAME), any()))
        .thenReturn(drsRequestCountMax);

    drsMetricsService = new DrsMetricsService(meterRegistry);
  }

  @Test
  void tryIncrementCurrentDrsRequestCount() {
    drsMetricsService.tryIncrementCurrentDrsRequestCount();
    assertEquals(
        1,
        currentDrsRequestCount.get(),
        "current request count is incremented when we are below the max");

    assertThrows(
        TooManyRequestsException.class,
        () -> drsMetricsService.tryIncrementCurrentDrsRequestCount());
    assertEquals(
        1,
        currentDrsRequestCount.get(),
        "current request count is untouched if too many open requests");
  }

  @Test
  void decrementCurrentDrsRequestCount() {
    drsMetricsService.decrementCurrentDrsRequestCount();
    assertEquals(-1, currentDrsRequestCount.get(), "current request count is decremented");
  }

  @Test
  void setDrsRequestMax() {
    var newMax = 20;
    drsMetricsService.setDrsRequestMax(newMax);
    assertEquals(newMax, drsRequestCountMax.get(), "maximum request count is set");
  }
}
