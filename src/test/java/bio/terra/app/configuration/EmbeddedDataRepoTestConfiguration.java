package bio.terra.app.configuration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
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
@EnableAutoConfiguration(
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      JdbcTemplateAutoConfiguration.class
    })
@ConfigurationProperties(prefix = "db.datarepo")
@ConditionalOnProperty(prefix = "datarepo", name = "testWithEmbeddedDatabase")
public class EmbeddedDataRepoTestConfiguration extends DataRepoJdbcConfiguration {

  @Autowired private DataSource embeddedDataSource;

  @Bean("dataRepoTransactionManager")
  @Override
  public PlatformTransactionManager getTransactionManager() {
    return new DataSourceTransactionManager(getDataSource());
  }

  @Override
  protected void configureDataSource() {
    try (Connection conn = embeddedDataSource.getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select count(*) from pg_extension where extname = 'pgcrypto'")) {
      resultSet.next();
      int count = resultSet.getInt("count");
      resultSet.close();
      if (count == 0) {
        statement.execute("create extension pgcrypto");
      }
    } catch (SQLException ex) {
      throw new RuntimeException("Could not install extension pgcrypto", ex);
    }
    dataSource =
        EmbeddedDatabaseInitialization.createConnectionPool(
            embeddedDataSource, poolMaxIdle, poolMaxTotal);
  }
}
