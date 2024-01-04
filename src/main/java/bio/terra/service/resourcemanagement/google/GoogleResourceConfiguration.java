package bio.terra.service.resourcemanagement.google;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties
@ConfigurationProperties(prefix = "google")
public record GoogleResourceConfiguration(
    String applicationName,
    long projectCreateTimeoutSeconds,
    int firestoreRetries,
    boolean allowReuseExistingBuckets,
    String secureFolderResourceId,
    String defaultFolderResourceId) {}
