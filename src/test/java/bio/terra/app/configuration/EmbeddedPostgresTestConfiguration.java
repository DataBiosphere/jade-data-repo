package bio.terra.app.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
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
public class EmbeddedPostgresTestConfiguration extends DataRepoJdbcConfiguration {

  private final Logger logger = LoggerFactory.getLogger(EmbeddedPostgresTestConfiguration.class);

  @Autowired private DataSource dataSource;

  @Bean("dataRepoTransactionManager")
  @Override
  public PlatformTransactionManager getTransactionManager() {
    return new DataSourceTransactionManager(getDataSource());
  }

  @Override
  public PoolingDataSource<PoolableConnection> getDataSource() {
    try (var conn = dataSource.getConnection()) {
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
    final ConnectionFactory connectionFactory = new DataSourceConnectionFactory(dataSource);

    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);

    final GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(poolMaxTotal);
    config.setMaxIdle(poolMaxIdle);

    final ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory, config);

    poolableConnectionFactory.setPool(connectionPool);

    return new PoolingDataSource<>(connectionPool);
  }
}
