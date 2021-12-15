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
  private int firestoreRetries;
  private boolean allowReuseExistingBuckets;
  private String secureFolderResourceId;

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

  public String getSecureFolderResourceId() {
    return secureFolderResourceId;
  }

  public void setSecureFolderResourceId(String secureFolderResourceId) {
    this.secureFolderResourceId = secureFolderResourceId;
  }
}
