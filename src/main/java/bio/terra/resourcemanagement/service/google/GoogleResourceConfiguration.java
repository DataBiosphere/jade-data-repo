package bio.terra.resourcemanagement.service.google;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@EnableConfigurationProperties
@Configuration
@ConfigurationProperties(prefix = "google")
public class GoogleResourceConfiguration {
    private String applicationName;
    private long projectCreateTimeoutSeconds;
    private String projectId;

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

    @Bean("firestore")
    public Firestore firestore() {
        return FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }
}
