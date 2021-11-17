package bio.terra.app.configuration;

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

    final GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
    config.setMaxIdle(poolMaxIdle);
    config.setMaxTotal(poolMaxTotal);

    final ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory, config);

    poolableConnectionFactory.setPool(connectionPool);

    return new PoolingDataSource<>(connectionPool);
  }
}
