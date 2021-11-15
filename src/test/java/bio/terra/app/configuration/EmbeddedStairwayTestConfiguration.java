package bio.terra.app.configuration;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableConfigurationProperties
@EnableTransactionManagement
@EnableAutoConfiguration(
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      JdbcTemplateAutoConfiguration.class
    })
@ConfigurationProperties(prefix = "db.stairway")
@ConditionalOnMissingClass({"bio.terra.app.configuration.IntegrationApplicationConfiguration"})
public class EmbeddedStairwayTestConfiguration extends StairwayJdbcConfiguration {

  @Autowired private DataSource embeddedDataSource;

  @Override
  protected void configureDataSource() {
    dataSource =
        EmbeddedDatabaseInitialization.initialize(embeddedDataSource, poolMaxIdle, poolMaxTotal);
  }
}
