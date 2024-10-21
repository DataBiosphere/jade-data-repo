package bio.terra.app.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Tag(Unit.TAG)
class DrsConfigurationTest {

  private static final int MAX_DRS_LOOKUPS = 3;
  private static final int NUM_DRS_RESOLUTION_THREADS = 5;

  @Test
  void drsResolutionThreadpool() {
    AsyncTaskExecutor genericExecutor =
        new DrsConfiguration(MAX_DRS_LOOKUPS, NUM_DRS_RESOLUTION_THREADS).drsResolutionThreadpool();
    assertThat(genericExecutor, instanceOf(ThreadPoolTaskExecutor.class));
    ThreadPoolTaskExecutor drsResolutionThreadpool = (ThreadPoolTaskExecutor) genericExecutor;
    assertThat(drsResolutionThreadpool.getCorePoolSize(), equalTo(NUM_DRS_RESOLUTION_THREADS));
    assertThat(drsResolutionThreadpool.getMaxPoolSize(), equalTo(NUM_DRS_RESOLUTION_THREADS));
    assertThat(drsResolutionThreadpool.getKeepAliveSeconds(), equalTo(0));
    assertThat(drsResolutionThreadpool.getQueueCapacity(), equalTo(NUM_DRS_RESOLUTION_THREADS));
  }
}
