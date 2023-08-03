package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

@ConfigurationProperties(prefix = "oauth")
@ConstructorBinding
public record OauthConfiguration(
    String schemeName, String loginEndpoint, String clientId, String clientSecret) {}
