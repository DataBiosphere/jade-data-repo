package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "sam")
public class SamConfiguration {
    private String basePath;
    private String stewardsGroupEmail;
    private String adminsGroupEmail;
    private int retryInitialWaitSeconds;
    private int retryMaximumWaitSeconds;
    private int operationTimeoutSeconds;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getStewardsGroupEmail() {
        return stewardsGroupEmail;
    }

    public void setStewardsGroupEmail(String stewardsGroupEmail) {
        this.stewardsGroupEmail = stewardsGroupEmail;
    }

    public String getAdminsGroupEmail() {
        return adminsGroupEmail;
    }

    public void setAdminsGroupEmail(String adminsGroupEmail) {
        this.adminsGroupEmail = adminsGroupEmail;
    }

    // SAM Retry notes:
    // We frequently experience socket timeouts with SAM, so we have implemented retry. The configuration controls
    // work like this. operationTimeoutSeconds is the maximum amount of time we allow for the SAM operation,
    // including the SAM operations and our retry sleep time. This is the upper bound on how long the entire
    // operation will take.
    //
    // The retry strategy is exponential backoff. The error wait starts at retryInitialWaitSeconds, and doubles
    // up to retryMaximumWaitSeconds. The retry stays at retryMaximumWaitSeconds until we reach the
    // operationTimeoutSeconds limit. Well, depending on the numbers you choose, we might hit operationTimeoutSeconds
    // before we reach retryMaximumSeconds.

    public int getRetryInitialWaitSeconds() {
        return retryInitialWaitSeconds;
    }

    public void setRetryInitialWaitSeconds(int retryInitialWaitSeconds) {
        this.retryInitialWaitSeconds = retryInitialWaitSeconds;
    }

    public int getRetryMaximumWaitSeconds() {
        return retryMaximumWaitSeconds;
    }

    public void setRetryMaximumWaitSeconds(int retryMaximumWaitSeconds) {
        this.retryMaximumWaitSeconds = retryMaximumWaitSeconds;
    }

    public int getOperationTimeoutSeconds() {
        return operationTimeoutSeconds;
    }

    public void setOperationTimeoutSeconds(int operationTimeoutSeconds) {
        this.operationTimeoutSeconds = operationTimeoutSeconds;
    }
}
