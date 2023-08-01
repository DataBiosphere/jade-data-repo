package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

@ConfigurationProperties(prefix = "ecm")
@ConstructorBinding
public record EcmConfiguration(String basePath, String rasIssuer) {}
