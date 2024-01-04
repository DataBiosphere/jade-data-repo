package bio.terra.app.configuration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.stairway")
@ConditionalOnProperty(
    prefix = "datarepo",
    name = "testWithEmbeddedDatabase",
    havingValue = "false",
    matchIfMissing = true)
public class StairwayJdbcConfiguration extends JdbcConfiguration {}
