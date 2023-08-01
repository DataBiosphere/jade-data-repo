package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

/* SAM Retry notes:
We frequently experience socket timeouts with SAM, so we have implemented retry. The
configuration controls work like this. operationTimeoutSeconds is the maximum amount of time we
allow for the SAM operation, including the SAM operations and our retry sleep time. This is the
upper bound on how long the entire operation will take.

The retry strategy is exponential backoff. The error wait starts at retryInitialWaitSeconds,
and doubles up to retryMaximumWaitSeconds. The retry stays at retryMaximumWaitSeconds until we
reach the operationTimeoutSeconds limit. Well, depending on the numbers you choose, we might hit
operationTimeoutSeconds before we reach retryMaximumSeconds.*/

@ConfigurationProperties(prefix = "sam")
@ConstructorBinding
public record SamConfiguration(
    String basePath,
    String stewardsGroupEmail,
    String adminsGroupEmail,
    int retryInitialWaitSeconds,
    int retryMaximumWaitSeconds,
    int operationTimeoutSeconds) {}
