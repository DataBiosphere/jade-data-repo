package bio.terra.app.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.BlockingRejectedExecutionHandler;
import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration.Threading;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Tag(Unit.TAG)
public class AzureResourceConfigurationTest {
  private static final int NUM_TABLE_THREADS = 3;
  private static final int MAX_QUEUE_SIZE = 5;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void azureTableThreadPool(boolean blockWhenSaturated) {
    Threading threading = new Threading(NUM_TABLE_THREADS, MAX_QUEUE_SIZE, blockWhenSaturated);
    AzureResourceConfiguration config =
        new AzureResourceConfiguration(null, null, 0, 0, null, null, threading);
    AsyncTaskExecutor genericExecutor = config.azureTableThreadpool();
    assertThat(genericExecutor, instanceOf(ThreadPoolTaskExecutor.class));
    ThreadPoolTaskExecutor azureTableThreadPool = (ThreadPoolTaskExecutor) genericExecutor;
    assertThat(azureTableThreadPool.getCorePoolSize(), equalTo(NUM_TABLE_THREADS));
    assertThat(azureTableThreadPool.getMaxPoolSize(), equalTo(NUM_TABLE_THREADS));
    assertThat(azureTableThreadPool.getKeepAliveSeconds(), equalTo(0));
    assertThat(azureTableThreadPool.getQueueCapacity(), equalTo(MAX_QUEUE_SIZE));
    RejectedExecutionHandler rejectedExecutionHandler =
        azureTableThreadPool.getThreadPoolExecutor().getRejectedExecutionHandler();
    if (blockWhenSaturated) {
      assertThat(rejectedExecutionHandler, instanceOf(BlockingRejectedExecutionHandler.class));
    } else {
      assertThat(rejectedExecutionHandler, instanceOf(ThreadPoolExecutor.AbortPolicy.class));
    }
  }
}
