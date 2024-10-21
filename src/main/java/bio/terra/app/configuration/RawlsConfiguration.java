package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rawls")
public record RawlsConfiguration(String basePath) {}
