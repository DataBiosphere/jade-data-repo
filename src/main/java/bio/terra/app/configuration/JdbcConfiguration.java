package bio.terra.app.configuration;

import java.util.Properties;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/** Base class for accessing JDBC configuration properties. */
public class JdbcConfiguration {
  private String uri;
  private String username;
  private String password;
  private String changesetFile;
  /**
   * Maximum number of database connections in the connection pool; -1 means no limit The goal of
   * these parameters is to prevent waiting for a database connection.
   */
  protected int poolMaxTotal;

  /** Maximum number of database connections to keep idle */
  protected int poolMaxIdle;

  // Not a property
  protected PoolingDataSource<PoolableConnection> dataSource;

  public String getUri() {
    return uri;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getChangesetFile() {
    return changesetFile;
  }

  public int getPoolMaxTotal() {
    return poolMaxTotal;
  }

  public int getPoolMaxIdle() {
    return poolMaxIdle;
  }

  // NOTE: even though the setters appear unused, the Spring infrastructure uses them to populate
  // the properties.
  public void setUri(String uri) {
    this.uri = uri;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setChangesetFile(String changesetFile) {
    this.changesetFile = changesetFile;
  }

  public void setPoolMaxTotal(int poolMaxTotal) {
    this.poolMaxTotal = poolMaxTotal;
  }

  public void setPoolMaxIdle(int poolMaxIdle) {
    this.poolMaxIdle = poolMaxIdle;
  }

  // Main use of the configuration is this pooling data source object.
  public PoolingDataSource<PoolableConnection> getDataSource() {
    // Lazy allocation of the data source
    if (dataSource == null) {
      configureDataSource();
    }
    return dataSource;
  }

  protected void configureDataSource() {
    final Properties props = new Properties();
    props.setProperty("user", getUsername());
    props.setProperty("password", getPassword());

    final ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(getUri(), props);

    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);

    final GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(poolMaxTotal);
    config.setMaxIdle(poolMaxIdle);
    final ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory, config);

    poolableConnectionFactory.setPool(connectionPool);

    dataSource = new PoolingDataSource<>(connectionPool);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("uri", uri)
        .append("username", username)
        // .append("password", password) NOTE: password is not printed; that avoids it showing up in
        // logs
        .toString();
  }
}
