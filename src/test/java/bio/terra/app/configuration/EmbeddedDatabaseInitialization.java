package bio.terra.app.configuration;

import java.time.Duration;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class EmbeddedDatabaseInitialization {
  public static PoolingDataSource<PoolableConnection> createConnectionPool(
      DataSource embeddedDataSource, int poolMaxIdle, int poolMaxTotal) {
    final ConnectionFactory connectionFactory = new DataSourceConnectionFactory(embeddedDataSource);

    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);

    // timeout for the testOnBorrow validation query
    poolableConnectionFactory.setValidationQueryTimeout(Duration.ofSeconds(5));
    // Always rebuild a connection after 30 mins; this prevents bad connections
    // from remaining in the pool indefinitely
    poolableConnectionFactory.setMaxConn(Duration.ofMinutes(30));

    final GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
    config.setMaxIdle(poolMaxIdle);
    config.setMaxTotal(poolMaxTotal);
    config.setTestOnBorrow(true);

    final ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory, config);

    poolableConnectionFactory.setPool(connectionPool);

    return new PoolingDataSource<>(connectionPool);
  }
}
