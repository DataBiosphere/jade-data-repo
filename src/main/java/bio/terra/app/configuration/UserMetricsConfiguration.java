package bio.terra.app.configuration;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

@ConfigurationProperties(prefix = "usermetrics")
public record UserMetricsConfiguration(
    String appId,
    String bardBasePath,
    Integer metricsReportingPoolSize,
    Integer syncRefreshIntervalSeconds,
    List<String> ignorePaths) {

  @Bean("metricsReportingThreadpool")
  public ExecutorService metricsPerformanceThreadpool() {
    return new ThreadPoolExecutor(
        metricsReportingPoolSize,
        metricsReportingPoolSize,
        0,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(metricsReportingPoolSize));
  }
}
