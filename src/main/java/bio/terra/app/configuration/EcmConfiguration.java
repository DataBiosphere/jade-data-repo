package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ecm")
public record EcmConfiguration(String basePath, String rasIssuer) {}
