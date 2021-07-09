package bio.terra.datarepo.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableConfigurationProperties
@EnableTransactionManagement
@ConfigurationProperties(prefix = "db.datarepo")
public class DataRepoJdbcConfiguration extends JdbcConfiguration {

  @Bean("dataRepoTransactionManager")
  public PlatformTransactionManager getTransactionManager() {
    return new DataSourceTransactionManager(getDataSource());
  }
}
