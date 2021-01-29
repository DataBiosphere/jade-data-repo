package bio.terra.service.resourcemanagement.google;

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
    private String parentResourceType;
    private String parentResourceId;
    private String singleDataProjectId;
    private String defaultFirestoreLocation;
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

    public String getParentResourceType() {
        return parentResourceType;
    }

    public void setParentResourceType(String parentResourceType) {
        this.parentResourceType = parentResourceType;
    }

    public String getParentResourceId() {
        return parentResourceId;
    }

    public void setParentResourceId(String parentResourceId) {
        this.parentResourceId = parentResourceId;
    }

    public String getSingleDataProjectId() {
        return singleDataProjectId;
    }

    public void setSingleDataProjectId(String singleDataProjectId) {
        this.singleDataProjectId = singleDataProjectId;
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

    public String getDefaultFirestoreLocation() {
        return defaultFirestoreLocation;
    }

    public void setDefaultFirestoreLocation(String defaultFirestoreLocation) {
        this.defaultFirestoreLocation = defaultFirestoreLocation;
    }

    // TODO: Is this used?
    @Bean("firestore")
    public Firestore firestore() {
        return FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }
}
