package bio.terra.service.resourcemanagement.google;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "google")
public class GoogleResourceConfiguration {
  private String applicationName;
  private long projectCreateTimeoutSeconds;
  private String projectId;
  private int firestoreRetries;
  private boolean allowReuseExistingProjects;
  private boolean allowReuseExistingBuckets;

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public long getProjectCreateTimeoutSeconds() {
    return projectCreateTimeoutSeconds;
  }

  public void setProjectCreateTimeoutSeconds(long projectCreateTimeoutSeconds) {
    this.projectCreateTimeoutSeconds = projectCreateTimeoutSeconds;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public boolean getAllowReuseExistingProjects() {
    return allowReuseExistingProjects;
  }

  public void setAllowReuseExistingProjects(boolean allowReuseExistingProjects) {
    this.allowReuseExistingProjects = allowReuseExistingProjects;
  }

  public boolean getAllowReuseExistingBuckets() {
    return allowReuseExistingBuckets;
  }

  public void setAllowReuseExistingBuckets(boolean allowReuseExistingBuckets) {
    this.allowReuseExistingBuckets = allowReuseExistingBuckets;
  }

  public int getFirestoreRetries() {
    return firestoreRetries;
  }

  public void setFirestoreRetries(int firestoreRetries) {
    this.firestoreRetries = firestoreRetries;
  }
}
