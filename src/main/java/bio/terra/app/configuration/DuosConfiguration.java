package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "duos")
public record DuosConfiguration(String basePath) {}
