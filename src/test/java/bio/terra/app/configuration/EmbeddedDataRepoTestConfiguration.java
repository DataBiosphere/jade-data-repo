package bio.terra.app.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
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
@ConfigurationProperties(prefix = "db.datarepo")
public class EmbeddedDataRepoTestConfiguration extends DataRepoJdbcConfiguration {

  @Autowired private DataSource embeddedDataSource;

  @Override
  protected void configureDataSource() {
    try (var conn = embeddedDataSource.getConnection()) {
      ResultSet resultSet =
          conn.createStatement()
              .executeQuery("select count(*) from pg_extension where extname = 'pgcrypto'");
      resultSet.next();
      int count = resultSet.getInt("count");
      if (count == 0) {
        conn.createStatement().execute("create extension pgcrypto");
      }
    } catch (SQLException ex) {
      throw new RuntimeException("Could not install extension pgcrypto", ex);
    }
    dataSource =
        EmbeddedDatabaseInitialization.initialize(embeddedDataSource, poolMaxIdle, poolMaxTotal);
  }
}
