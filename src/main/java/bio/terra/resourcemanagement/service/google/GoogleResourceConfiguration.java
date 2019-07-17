package bio.terra.resourcemanagement.service.google;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "google")
public class GoogleResourceConfiguration {
    private String applicationName;
    private long projectCreateTimeoutSeconds;
    private String projectId;
    private String coreBillingAccount;

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

    public String getCoreBillingAccount() {
        return coreBillingAccount;
    }

    public void setCoreBillingAccount(String coreBillingAccount) {
        this.coreBillingAccount = coreBillingAccount;
    }

    @Bean("firestore")
    public Firestore firestore() {
        return FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }
}
