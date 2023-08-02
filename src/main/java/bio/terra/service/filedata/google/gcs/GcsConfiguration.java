package bio.terra.service.filedata.google.gcs;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.context.annotation.Profile;

/**
 * The theory of operation here is that each implementation of a pdao will specify a configuration.
 * That way, implementation specifics can be separated from the interface. We'll see if it works out
 * that way.
 */
@ConstructorBinding
@Profile("google")
@ConfigurationProperties(prefix = "datarepo.gcs")
public record GcsConfiguration(
    String bucket, String region, int connectTimeoutSeconds, int readTimeoutSeconds) {}
