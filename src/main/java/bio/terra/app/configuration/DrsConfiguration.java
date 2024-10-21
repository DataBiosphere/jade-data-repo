package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@ConfigurationProperties(prefix = "drs")
public record DrsConfiguration(int maxDrsLookups, int numDrsResolutionThreads) {

  @Bean("drsResolutionThreadpool")
  public AsyncTaskExecutor drsResolutionThreadpool() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(numDrsResolutionThreads);
    executor.setMaxPoolSize(numDrsResolutionThreads);
    executor.setKeepAliveSeconds(0);
    executor.setQueueCapacity(numDrsResolutionThreads);
    executor.setThreadNamePrefix("drs-resolution-thread-");
    executor.initialize();
    return executor;
  }
}
