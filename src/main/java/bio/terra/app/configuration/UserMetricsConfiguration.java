package bio.terra.app.configuration;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "usermetrics")
public class UserMetricsConfiguration {

  private String appId;
  private String bardBasePath;
  private Integer metricsReportingPoolSize;

  private Integer syncRefreshInterval;
  private List<String> ignorePaths;

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getBardBasePath() {
    return bardBasePath;
  }

  public void setBardBasePath(String bardBasePath) {
    this.bardBasePath = bardBasePath;
  }

  public Integer getMetricsReportingPoolSize() {
    return metricsReportingPoolSize;
  }

  public Integer getSyncRefreshInterval() {
    return syncRefreshInterval;
  }

  public void setMetricsReportingPoolSize(Integer metricsReportingPoolSize) {
    this.metricsReportingPoolSize = metricsReportingPoolSize;
  }

  public void setSyncRefreshInterval(Integer syncRefreshInterval) {
    this.syncRefreshInterval = syncRefreshInterval;
  }

  public List<String> getIgnorePaths() {
    return ignorePaths;
  }

  public void setIgnorePaths(List<String> ignorePaths) {
    this.ignorePaths = ignorePaths;
  }

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
