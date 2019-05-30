package bio.terra.service.google;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

//@EnableConfigurationProperties
@ConfigurationProperties(prefix = "google")
public class GoogleResourceConfiguration {
    private String applicationName;
    private long projectCreateTimeoutSeconds;

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
}
