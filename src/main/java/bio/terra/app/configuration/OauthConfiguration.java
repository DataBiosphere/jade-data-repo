package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "oauth")
public record OauthConfiguration(
    String schemeName, String loginEndpoint, String clientId, String clientSecret) {}
