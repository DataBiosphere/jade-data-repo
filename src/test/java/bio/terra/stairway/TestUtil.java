package bio.terra.stairway;

import bio.terra.configuration.StairwayJdbcConfiguration;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class TestUtil {
    private TestUtil() {
    }

    static final Integer intValue = Integer.valueOf(22);
    static final String strValue = "testing 1 2 3";
    static final Double dubValue = new Double(Math.PI);
    static final String errString = "Something bad happened";
    static final String flightId = "aaa111";
    static final String ikey = "ikey";
    static final String skey = "skey";
    static final String fkey = "fkey";
    static final String wikey = "wikey";
    static final String wskey = "wskey";
    static final String wfkey = "wfkey";

    // debug output control (until we have logging configured)
    static final boolean debugOutput = false;

    static void debugWrite(String msg) {
        if (debugOutput) {
            System.out.println(msg);
        }
    }

    static PoolingDataSource<PoolableConnection> setupDataSource(StairwayJdbcConfiguration jdbcConfiguration) {
        Properties props = new Properties();
        props.setProperty("user", jdbcConfiguration.getUsername());
        props.setProperty("password", jdbcConfiguration.getPassword());

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcConfiguration.getUri(), props);

        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        poolableConnectionFactory.setPool(connectionPool);

        return new PoolingDataSource<>(connectionPool);
    }

    static Stairway setupStairway(StairwayJdbcConfiguration jdbcConfiguration) {
        PoolingDataSource<PoolableConnection> dataSource;
        dataSource = TestUtil.setupDataSource(jdbcConfiguration);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        return new Stairway(executorService, dataSource, true, null);
    }
}
