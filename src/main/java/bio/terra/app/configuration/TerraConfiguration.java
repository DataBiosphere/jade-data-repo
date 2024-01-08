package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "terra")
public record TerraConfiguration(String basePath) {}
