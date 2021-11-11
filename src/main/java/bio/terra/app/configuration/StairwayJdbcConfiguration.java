package bio.terra.app.configuration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.stairway")
@ConditionalOnMissingClass({"bio.terra.app.configuration.EmbeddedStairwayTestConfiguration"})
public class StairwayJdbcConfiguration extends JdbcConfiguration {}
